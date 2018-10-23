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
		private readonly Options _options;
		private string _tableSql;
		private string _selectColumnsSql;
		private List<Column> _primaryKeys;
		private string _insertClickHouseSql;

		protected Migrator(Options options)
		{
			_options = options;
		}

		protected abstract string ConvertToClickHouserDataType(string type);

		protected virtual ClickHouseConnection CreateClickHouseConnection(string database = null)
		{
			string connectStr = $"Compress=True;CheckCompressedHash=False;Compressor=lz4;Host={_options.Host};Port={_options.Port};Database=system;User={_options.User};Password={_options.Password}";
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

		protected abstract List<Column> GetColumns(string host, int port, string user, string pass, string database, string table);

		protected abstract string GenerateTableSql(string database, string table);

		protected abstract string GenerateSelectColumnsSql(List<Column> columns);

		public void Run()
		{
			if (!Init())
			{
				return;
			}

			Log.Logger.Verbose($"Thread: {_options.Thread}, Batch: {_options.Batch}.");

			PrepareClickHouse();

			Interlocked.Exchange(ref _batch, -1);
			Interlocked.Exchange(ref _counter, 0);

			Migrate();
		}

		private bool Init()
		{
			var columns = GetColumns();

			_primaryKeys = columns.Where(c => c.IsPrimary).ToList();

			if (_primaryKeys.Count == 0 && _options.Thread > 1)
			{
				_options.Thread = 1;
				Log.Warning($"Table: {_options.SourceTable} contains no primary key, can't support parallel mode.");
			}

			if (_primaryKeys.Count == 0 && _options.OrderBy.Count() == 0)
			{
				var msg = "Source table uncontains primary, and options uncontains order by.";
				Log.Error(msg);
				return false;
			}

			if (_options.OrderBy.Count() > 0)
			{
				foreach (var column in _options.OrderBy)
				{
					if (!columns.Any(cl => cl.Name.ToLower() == column.ToLower()))
					{
						var msg = $"Can't find order by column: {column} in source table.";
						Log.Error(msg);
						return false;
					}
				}
			}

			_tableSql = GenerateTableSql(_options.SourceDatabase, _options.SourceTable);

			_selectColumnsSql = GenerateSelectColumnsSql(columns);

			var insertColumnsSql = string.Join(", ", columns.Select(c => $"{(_options.IgnoreCase ? c.Name.ToLowerInvariant() : c.Name)}"));
			_insertClickHouseSql = $"INSERT INTO {_options.GetTargetTable()} ({insertColumnsSql}) VALUES @bulk;";

			return true;
		}

		private void Migrate()
		{
			if (_primaryKeys.Count <= 0 || _options.Mode.ToLower() == "sequential")
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
			using (var clickHouseConn = CreateClickHouseConnection(_options.GetTargetDatabase()))
			{
				var command = clickHouseConn.CreateCommand($"SELECT COUNT(*) FROM {_options.GetTargetDatabase()}.{_options.GetTargetTable()}");
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
			using (var clickHouseConn = CreateClickHouseConnection(_options.GetTargetDatabase()))
			using (var conn = CreateDbConnection(_options.SourceHost, _options.SourcePort, _options.SourceUser, _options.SourcePassword, _options.SourceDatabase))
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

						if (list.Count % _options.Batch == 0)
						{
							watch.Stop();

							if (_options.TracePerformance)
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

			Parallel.For(0, _options.Thread, new ParallelOptions { MaxDegreeOfParallelism = _options.Thread }, (i) =>
			{
				using (var clickHouseConn = CreateClickHouseConnection(_options.GetTargetDatabase()))
				using (var conn = CreateDbConnection(_options.SourceHost, _options.SourcePort, _options.SourceUser, _options.SourcePassword, _options.SourceDatabase))
				{
					if (conn.State != ConnectionState.Open)
					{
						conn.Open();
					}

					Stopwatch watch = new Stopwatch();

					while (true)
					{
						Interlocked.Increment(ref _batch);

						var command = TracePerformance(watch, () => GenerateBatchQueryCommand(conn, _primaryKeys, _selectColumnsSql, _tableSql, _batch, _options.Batch), "Construct query data command cost: {0} ms.");
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

							if (count % _options.Batch == 0)
							{
								var costTime = progressWatch.ElapsedMilliseconds / 1000;
								if (costTime > 0)
								{
									Log.Logger.Verbose($"Total: {_counter}, Speed: {_counter / costTime} Row/Sec.");
								}
							}
						}
						if (command.Item2 < _options.Batch)
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
			return GetColumns(_options.SourceHost, _options.SourcePort, _options.SourceUser, _options.SourcePassword, _options.SourceDatabase, _options.SourceTable);
		}

		private T TracePerformance<T>(Stopwatch watch, Func<T> func, string message)
		{
			if (!_options.TracePerformance || watch == null)
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
			if (!_options.TracePerformance || watch == null)
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
					//Console.WriteLine(_insertClickHouseSql);
					command.ExecuteNonQuery();
				}
			});
		}

		private void PrepareClickHouse()
		{
			using (var conn = CreateClickHouseConnection())
			{
				conn.Execute($"CREATE DATABASE IF NOT EXISTS {_options.GetTargetDatabase()};");
				conn.Execute($"USE {_options.GetTargetDatabase()};");

				if (_options.Drop)
				{
					conn.Execute($"DROP TABLE IF EXISTS {_options.GetTargetTable()};");
				}

				conn.Execute(GenerateCreateClickHouseTableSql());
			}
		}

		private string GenerateCreateClickHouseTableSql()
		{
			var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {_options.GetTargetTable()} (");

			foreach (var column in GetColumns())
			{
				var clickhouseDataType = ConvertToClickHouserDataType(column.DataType);
				stringBuilder.Append($"{(_options.IgnoreCase ? column.Name.ToLowerInvariant() : column.Name)} {clickhouseDataType}, ");
			}
			stringBuilder.Remove(stringBuilder.Length - 2, 2);
			var orderby = _options.OrderBy.Count() > 0 ? string.Join(", ", _options.OrderBy.Select(k => _options.IgnoreCase ? k.ToLowerInvariant() : k)) : string.Join(", ", _primaryKeys.Select(k => _options.IgnoreCase ? k.Name.ToLowerInvariant() : k.Name));

			stringBuilder.Append($") ENGINE = MergeTree ORDER BY ({orderby}) SETTINGS index_granularity = 8192");

			var sql = stringBuilder.ToString();
			return sql;
		}
	}
}
