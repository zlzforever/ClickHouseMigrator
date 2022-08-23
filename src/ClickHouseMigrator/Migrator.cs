using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClickHouse.Ado;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator
{
	public abstract class Migrator : IMigrator
	{
		protected ClickHouseOptions Options;
		private string _database;
		private string _table;
		private string _connectionString;
		private DateTime _start;
		private List<ColumnDefine> _columns;
		protected abstract List<ColumnDefine> FetchTablesColumns();
		protected abstract (dynamic[][] Data, int Length) FetchRows(int count);
		protected abstract void Initialize(IConfiguration configuration);

		/// <summary>
		/// 12,000,000
		/// pure read:                     Complete: 18.056909 s, Speed: 676114
		/// read + GetValues:              Complete: 25.391505 s, Speed: 481089
		/// read + GetValues + Multi task: Complete: 32 s, Speed: 481089 
		/// all：                          Complete: 47.654823 s, Speed: 127996
		/// </summary>
		/// <param name="args"></param>
		/// <returns></returns>
		public Task RunAsync(params string[] args)
		{
			InitializeOptions(args);

			var externalConfiguration = GetExternalConfiguration(args);
			Initialize(externalConfiguration);

			_connectionString =
				$"Host={Options.Host};Database=default;Port={Options.Port};User={Options.User};Password={Options.Password};Compress=True;Compressor=lz4;SocketTimeout=10000;CheckCompressedHash=False;BufferSize=32768;";

			_database = string.IsNullOrWhiteSpace(Options.Database) ? Options.SourceDatabase : Options.Database;
			_table = string.IsNullOrWhiteSpace(Options.Table) ? Options.SourceTable : Options.Table;

			if (string.IsNullOrWhiteSpace(_database))
			{
				Logger.Error("Target database should not be null/empty");
				return Task.CompletedTask;
			}

			if (string.IsNullOrWhiteSpace(_table))
			{
				Logger.Error("Target table should not be null/empty");
				return Task.CompletedTask;
			}

			if (Options.Thread <= 0)
			{
				Logger.Error("Thread should greater than 0");
				return Task.CompletedTask;
			}

			_columns = FetchTablesColumns();
			
			if (Options.Lowercase)
			{
				foreach (var column in _columns)
				{
					column.Name = column.Name.ToLowerInvariant();
				}

				_database = _database.ToLowerInvariant();
				_table = _table.ToLowerInvariant();
			}

			InitializeTable();

			_start = DateTime.Now;

			var rows = Migrate();

			var end = DateTime.Now;
			var totalSeconds = (int) (end - _start).TotalSeconds;
			var speed = totalSeconds == 0 ? rows : (int) (rows / totalSeconds);
			Console.WriteLine();
			Console.WriteLine($"Elapsed {totalSeconds} sec. Processed {rows} rows ({speed} rows/s.)");
			return Task.CompletedTask;
		}

		private IConfiguration GetExternalConfiguration(string[] args)
		{
			var configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.AddCommandLine(args, GetSwitchMappings());
			return configurationBuilder.Build();
		}

		protected virtual Dictionary<string, string> GetSwitchMappings()
		{
			return null;
		}

		protected virtual string GenerateCreateTableSql()
		{
			var columnsSql = string.Join(", ", _columns.Select(x => $"`{x.Name}` {x.DataType}"));

			var primaryColumns = _columns.Where(x => x.IsPrimary).ToList();
			var primaryColumnsSql = string.Join(", ", primaryColumns.Select(x => $"`{x.Name}`"));

			//todo: As SQL Server columnStores does not have Primary Key, think about howto create the PK and OrderBy in CH
			var primarySql =
				primaryColumns.Count == 0
					? " ORDER BY tuple()"
					: $"PRIMARY KEY ({primaryColumnsSql}) ORDER BY({primaryColumnsSql})";
			var sql =
				$"CREATE TABLE IF NOT EXISTS `{_database}`.`{_table}` ({columnsSql}) ENGINE = MergeTree() {primarySql}";
			return sql;
		}

		private long Migrate()
		{
			var columnNames = _columns.Select(x => $"`{x.Name}`").ToArray();

			long total = 0;
			var columnsSql = string.Join(", ", columnNames);
			var insertSql = $"INSERT INTO {_database}.{_table} ({columnsSql}) VALUES @bulk;";
			var tasks = new Task[Options.Thread];

			for (int i = 0; i < Options.Thread; ++i)
			{
				var task = Task.Factory.StartNew(() =>
				{
					try
					{
						using var conn = new ClickHouseConnection(_connectionString);
						conn.Open();

						while (true)
						{
							(dynamic[][] Data, int Length) result;
							lock (this)
							{
								result = FetchRows(Options.Batch);
								total += result.Length;
							}

							if (result.Length == 0)
							{
								break;
							}

							using var command = conn.CreateCommand();
							command.CommandText = insertSql;

							var data = result.Length == Options.Batch
								? result.Data
								: result.Data.Where(x => x != null);
							var dataList = data.ToList();
							for (int i = 0; i < dataList.Count; i++)
							{
								var row = dataList[i];
								for (int j = 0; j < row.Length; j++)
								{
									var col = row[j];
									if (((object)col).GetType() == typeof(decimal))
										row[j] = (double)col;
								}
							}
							command.Parameters.Add(new ClickHouseParameter
							{
								ParameterName = "bulk",
								Value = dataList
							});

							//todo fails if there is a column if NULL
							//as Nullable columns impacts performance on ClickHouse, it would have to be coalesced() at source
							command.ExecuteNonQuery();

							if (result.Length < Options.Batch)
							{
								break;
							}
						}
					}
					catch (Exception e)
					{
						Logger.Error($"{e}");
					}
				});
				tasks[i] = task;
			}

			var cancellationTokenSource = new CancellationTokenSource();

			var progress = Task.Factory.StartNew(async () =>
			{
				string msg = null;
				while (!cancellationTokenSource.IsCancellationRequested)
				{
					//Progress: 2.10 million rows, 100.54 MB (709.92 thousand rows/s., 34.03 MB/s
					var end = DateTime.Now;
					var totalSeconds = (int) (end - _start).TotalSeconds;

					lock (this)
					{
						var speed = totalSeconds == 0 ? total : (int) (total / totalSeconds);
						if (!string.IsNullOrWhiteSpace(msg))
						{
							ClearLine(msg.Length);
						}

						msg = $"Progress: {total} rows ({speed} rows/s.)";
						Console.Write(msg);
					}

					await Task.Delay(1500, cancellationTokenSource.Token);
				}

				if (!string.IsNullOrWhiteSpace(msg))
				{
					ClearLine(msg.Length);
				}
			}, cancellationTokenSource.Token);

			Task.WaitAll(tasks);

			cancellationTokenSource.Cancel();
			Task.WaitAll(progress);

			return total;
		}

		private void ClearLine(int count)
		{
			for (int k = 0; k < count; ++k)
			{
				Console.Write('\b');
			}

			for (int k = 0; k < count; ++k)
			{
				Console.Write(' ');
			}

			for (int k = 0; k < count; ++k)
			{
				Console.Write('\b');
			}
		}

		private void InitializeOptions(string[] args)
		{
			var configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.AddCommandLine(args, new Dictionary<string, string>
			{
				{"--host", "Host"},
				{"--port", "Port"},
				{"--user", "User"},
				{"--password", "Password"},
				{"--database", "Database"},
				{"--table", "Table"},
				{"--drop-table", "DropTable"},
				{"--batch", "Batch"},
				{"--src-host", "SourceHost"},
				{"--src-port", "SourcePort"},
				{"--src-user", "SourceUser"},
				{"--src-password", "SourcePassword"},
				{"--src-database", "SourceDatabase"},
				{"--src-table", "SourceTable"},
				{"--file", "File"}
			});
			var configuration = configurationBuilder.Build();
			Options = new ClickHouseOptions(configuration);
		}

		protected virtual void InitializeTable()
		{
			var conn = new ClickHouseConnection(_connectionString);
			conn.Open();

			var command = conn.CreateCommand();
			command.CommandText = $"CREATE DATABASE IF NOT EXISTS {_database};";
			command.ExecuteNonQuery();

			if (Options.DropTable)
			{
				var sql = $"DROP TABLE IF EXISTS {_database}.{_table};";
				Logger.Warning(sql);

				command = conn.CreateCommand();
				command.CommandText = sql;
				command.ExecuteNonQuery();
			}

			command = conn.CreateCommand();
			command.CommandText = GenerateCreateTableSql();
			command.ExecuteNonQuery();
		}
	}
}