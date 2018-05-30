using ClickHouse.Ado;
using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using Serilog;
using Polly.Retry;
using Polly;

namespace ClickHouseMigrator.Impl
{
	public abstract class Migrator : IMigrator
	{
		private static readonly RetryPolicy RetryPolicy = Policy.Handle<Exception>().Retry(5, (ex, count) =>
		{
			Log.Logger.Error($"Insert data to clickhouse failed [{count}]: {ex}");
		});
		private static int _batch;
		private static int _counter;
		private readonly Arguments _arguments;
		private string _tableSql;
		private string _selectColumnsSql;
		private List<Column> _primaryKeys;
		private string _insertClickHouseSql;

		protected Migrator(Arguments arguments)
		{
			_arguments = arguments;
		}

		protected abstract string ConvertToClickHouserDataType(string type);

		protected virtual ClickHouseConnection CreateClickHouseConnection(string database = null)
		{
			string connectStr = $"Compress=True;CheckCompressedHash=False;Compressor=lz4;Host={_arguments.Host};Port={_arguments.Port};Database=system;User={_arguments.User};Password={_arguments.Password}";
			var settings = new ClickHouseConnectionSettings(connectStr);
			var cnn = new ClickHouseConnection(settings);
			cnn.Open();

			if (!string.IsNullOrWhiteSpace(database))
			{
				cnn.Execute($"USE {database};");
			}
			return cnn;
		}

		protected abstract Tuple<IDbCommand, int> GenerateBatchQueryCommand(IDbConnection conn, List<Column> primaryKeys, string selectColumnsSql, string tableSql, int batch, int batchCount);

		protected abstract string GenerateQueryAllSql(string selectColumnsSql, string tableSql);

		protected abstract IDbConnection CreateDbConnection(string host, int port, string user, string pass, string database);

		protected abstract List<Column> GetColumns(IDbConnection conn, string database, string table);

		protected abstract string GenerateTableSql(string database, string table);

		protected abstract string GenerateSelectColumnsSql(List<Column> columns);

		public void Run()
		{
			Init();

			Log.Logger.Verbose($"Thread: {_arguments.Thread}, Batch: {_arguments.Batch}.");

			PrepareClickHouse();

			Interlocked.Exchange(ref _batch, -1);
			Interlocked.Exchange(ref _counter, 0);

			Migrate();
		}

		private void Init()
		{
			_primaryKeys = GetColumns().Where(c => c.IsPrimary).ToList();

			if (_primaryKeys.Count == 0 && _arguments.Thread > 1)
			{
				_arguments.Thread = 1;
				Log.Warning($"Table: {_arguments.SourceTable} contains no primary key, can't support parallel mode.");
			}

			_tableSql = GenerateTableSql(_arguments.SourceDatabase, _arguments.SourceTable);

			_selectColumnsSql = GenerateSelectColumnsSql(GetColumns());

			var insertColumnsSql = GenerateInsertColumnsSql(GetColumns(), _arguments.MigrateDateColumnName, _arguments.IgnoreCase);

			_insertClickHouseSql = $"INSERT INTO {_arguments.GetTargetTable()} ({insertColumnsSql}) VALUES @bulk;";
		}

		protected abstract string GenerateInsertColumnsSql(List<Column> columns, string migrateDateColumnName, bool ignoreCase);

		private void Migrate()
		{
			if (_primaryKeys.Count <= 0 || _arguments.Mode.ToLower() == "sequential")
			{
				SequentialMigrate();
			}
			else
			{
				ParallelMigrate();
			}

			PrintReport();
		}

		private void PrintReport()
		{
			using (var clickHouseConn = CreateClickHouseConnection(_arguments.GetTargetDatabase()))
			{
				var command = clickHouseConn.CreateCommand($"SELECT COUNT(*) FROM {_arguments.GetTargetDatabase()}.{_arguments.GetTargetTable()}");
				using (var reader = command.ExecuteReader())
				{
					reader.ReadAll(x =>
					{
						Log.Logger.Verbose($"Migrate {x.GetValue(0)} rows.");
					});
				}
			}
		}

		private void SequentialMigrate()
		{
			Stopwatch progressWatch = new Stopwatch();
			progressWatch.Start();
			using (var clickHouseConn = CreateClickHouseConnection(_arguments.GetTargetDatabase()))
			using (var conn = CreateDbConnection(_arguments.SourceHost, _arguments.SourcePort, _arguments.SourceUser, _arguments.SourcePassword, _arguments.SourceDatabase))
			{
				if (conn.State != ConnectionState.Open)
				{
					conn.Open();
				}

				Stopwatch watch = new Stopwatch();

				using (var reader = conn.ExecuteReader(GenerateQueryAllSql(_selectColumnsSql, _tableSql)))
				{
					var list = new List<dynamic[]>();
					watch.Restart();

					while (reader.Read())
					{
						list.Add(reader.ToArray());
						Interlocked.Increment(ref _counter);

						if (list.Count % _arguments.Batch == 0)
						{
							watch.Stop();

							if (_arguments.TracePerformance)
							{
								Log.Logger.Debug($"Read and convert data cost: {watch.ElapsedMilliseconds} ms.");
							}

							TracePerformance(watch, () => InsertDataToClickHouse(clickHouseConn, list), "Insert data to clickhouse cost: {0} ms.");
							list.Clear();
							var costTime = progressWatch.ElapsedMilliseconds / 1000;
							if (costTime > 0)
							{
								Log.Logger.Verbose($"Total: {_counter}, Speed: {_counter / costTime} Row/Sec.");
							}

							watch.Restart();
						}
					}
					if (list.Count > 0)
					{
						InsertDataToClickHouse(clickHouseConn, list);
					}
					list.Clear();
				}
			}
			var finalCostTime = progressWatch.ElapsedMilliseconds / 1000;
			Log.Logger.Verbose($"Total: {_counter}, Speed: {_counter / finalCostTime} Row/Sec.");
		}

		private void ParallelMigrate()
		{
			Stopwatch progressWatch = new Stopwatch();
			progressWatch.Start();

			Parallel.For(0, _arguments.Thread, new ParallelOptions { MaxDegreeOfParallelism = _arguments.Thread }, (i) =>
			{
				using (var clickHouseConn = CreateClickHouseConnection(_arguments.GetTargetDatabase()))
				using (var conn = CreateDbConnection(_arguments.SourceHost, _arguments.SourcePort, _arguments.SourceUser, _arguments.SourcePassword, _arguments.SourceDatabase))
				{
					if (conn.State != ConnectionState.Open)
					{
						conn.Open();
					}

					Stopwatch watch = new Stopwatch();

					while (true)
					{
						Interlocked.Increment(ref _batch);

						var command = TracePerformance(watch, () => GenerateBatchQueryCommand(conn, _primaryKeys, _selectColumnsSql, _tableSql, _batch, _arguments.Batch), "Construct query data command cost: {0} ms.");
						if (command.Item2 == 0)
						{
							Log.Logger.Information($"Thread {i} exit.");
							break;
						}
						using (var reader = TracePerformance(watch, () => command.Item1.ExecuteReader(), "Query data from source database cost: {0} ms."))
						{
							int count = 0;
							var rows = TracePerformance(watch, () =>
							{
								var list = new List<dynamic[]>();

								while (reader.Read())
								{
									list.Add(reader.ToArray());
									count = Interlocked.Increment(ref _counter);
								}
								return list;
							}, "Read and convert data cost: {0} ms.");
							TracePerformance(watch, () => InsertDataToClickHouse(clickHouseConn, rows), "Insert data to clickhouse cost: {0} ms.");
							rows.Clear();

							if (count % 10000 == 0)
							{
								var costTime = progressWatch.ElapsedMilliseconds / 1000;
								if (costTime > 0)
								{
									Log.Logger.Verbose($"Total: {_counter}, Speed: {_counter / costTime} Row/Sec.");
								}
							}
						}
						if (command.Item2 < _arguments.Batch)
						{
							Log.Logger.Information($"Thread {i} exit.");
							break;
						}
					}
				}
			});
			var finalCostTime = progressWatch.ElapsedMilliseconds / 1000;
			Log.Logger.Verbose($"Total: {_counter}, Speed: {_counter / finalCostTime} Row/Sec.");
		}

		private List<Column> GetColumns()
		{
			using (var conn = CreateDbConnection(_arguments.SourceHost, _arguments.SourcePort, _arguments.SourceUser, _arguments.SourcePassword, _arguments.SourceDatabase))
			{
				return GetColumns(conn, _arguments.SourceDatabase, _arguments.SourceTable);
			}
		}

		private T TracePerformance<T>(Stopwatch watch, Func<T> func, string message)
		{
			if (!_arguments.TracePerformance || watch == null)
			{
				return func();
			}
			watch.Restart();
			var t = func();
			watch.Stop();
			Log.Logger.Debug(string.Format(message, watch.ElapsedMilliseconds));
			return t;
		}

		private void TracePerformance(Stopwatch watch, Action action, string message)
		{
			if (!_arguments.TracePerformance || watch == null)
			{
				action();
				return;
			}

			watch.Restart();
			action();
			watch.Stop();
			Log.Logger.Debug(string.Format(message, watch.ElapsedMilliseconds));
		}

		private void InsertDataToClickHouse(ClickHouseConnection clickHouseConn, List<dynamic[]> list)
		{
			if (list == null || list.Count == 0)
			{
				return;
			}
			RetryPolicy.ExecuteAndCapture(() =>
			{
				using (var command = clickHouseConn.CreateCommand())
				{
					command.CommandText = _insertClickHouseSql;
					command.Parameters.Add(new ClickHouseParameter
					{
						ParameterName = "bulk",
						Value = list
					});
					command.ExecuteNonQuery();
				}
			});
		}

		private void PrepareClickHouse()
		{
			using (var conn = CreateClickHouseConnection())
			{
				conn.Execute($"CREATE DATABASE IF NOT EXISTS {_arguments.GetTargetDatabase()};");
				conn.Execute($"USE {_arguments.GetTargetDatabase()};");

				if (_arguments.Drop)
				{
					conn.Execute($"DROP TABLE IF EXISTS {_arguments.GetTargetTable()};");
				}

				conn.Execute(GenerateCreateClickHouseTableSql());
			}
		}

		private string GenerateCreateClickHouseTableSql()
		{
			var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {_arguments.GetTargetTable()} (");

			foreach (var column in GetColumns())
			{
				var clickhouseDataType = ConvertToClickHouserDataType(column.DataType);
				stringBuilder.Append($"{column.Name} {clickhouseDataType}, ");
			}

			stringBuilder.Append($"{_arguments.MigrateDateColumnName} Date");

			var primaryKeys = _primaryKeys.Count == 0 ? new[] { _arguments.MigrateDateColumnName } : _primaryKeys.Select(k => k.Name);

			stringBuilder.Append($") ENGINE = MergeTree({_arguments.MigrateDateColumnName}, ({string.Join(", ", primaryKeys)}), 8192);");
			var sql = stringBuilder.ToString();
			return sql;
		}
	}
}
