using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
		protected Lazy<List<ColumnDefine>> Columns => new Lazy<List<ColumnDefine>>(FetchTablesColumns);
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

			if (Options.Lowercase)
			{
				foreach (var column in Columns.Value)
				{
					column.Name = column.Name.ToLower();
				}

				_database = _database.ToLower();
				_table = _table.ToLower();
			}

			InitializeTable();

			var stopwatch = new Stopwatch();
			stopwatch.Start();

			var rows = Migrate();

			stopwatch.Stop();
			var totalSeconds = (int) (stopwatch.ElapsedMilliseconds / 1000);
			var speed = totalSeconds == 0 ? rows : (int) (rows / totalSeconds);
			Console.WriteLine();
			Logger.Information($"Complete {totalSeconds} s, Speed {speed} rows/s");
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
			var columnsSql = string.Join(", ", Columns.Value.Select(x => $"`{x.Name}` {x.DataType}"));

			var primaryColumns = Columns.Value.Where(x => x.IsPrimary).ToList();
			var primaryColumnsSql = string.Join(", ", primaryColumns.Select(x => $"`{x.Name}`"));
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
			var columnNames = Columns.Value.Select(x => $"`{x.Name}`").ToArray();

			long total = 0;
			var columnsSql = string.Join(", ", columnNames);
			var insertSql = $"INSERT INTO {_database}.{_table} ({columnsSql}) VALUES @bulk;";
			var tasks = new Task[Options.Thread];

			var j = 0;
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
							command.Parameters.Add(new ClickHouseParameter
							{
								ParameterName = "bulk",
								Value = data
							});

							command.ExecuteNonQuery();

							lock (this)
							{
								j++;
								Console.Write("-");
								if (j >= Program.Line.Length)
								{
									for (int k = 0; k < Program.Line.Length; ++k)
									{
										Console.Write('\b');
									}

									for (int k = 0; k < Program.Line.Length; ++k)
									{
										Console.Write(' ');
									}

									for (int k = 0; k < Program.Line.Length; ++k)
									{
										Console.Write('\b');
									}

									j = 0;
								}
							}

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

			Task.WaitAll(tasks);

			return total;
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
				{"--file", "File"},
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