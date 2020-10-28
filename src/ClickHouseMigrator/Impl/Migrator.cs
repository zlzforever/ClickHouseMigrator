using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClickHouse.Client.ADO;
using ClickHouse.Client.Copy;
using Dapper;
using Microsoft.Extensions.Configuration;
using Serilog;
using Serilog.Core;

namespace ClickHouseMigrator.Impl
{
	public abstract class Migrator : IMigrator
	{
		private ClickHouseOptions _options;

		protected abstract Dictionary<string, ColumnDefine> FetchTablesColumns();
		protected abstract List<object[]> FetchRows(int count);

		protected abstract Dictionary<string, string> GetSwitchMappings();

		protected abstract void Initialize(IConfiguration configuration);

		public async Task RunAsync(params string[] args)
		{
			InitializeClickHouseOptions(args);

			var configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.AddCommandLine(args, GetSwitchMappings());
			var configuration = configurationBuilder.Build();

			Initialize(configuration);

			var columns = FetchTablesColumns();

			await using var cnn = new ClickHouseConnection($"Host={_options.Host};Driver=Binary;");
			cnn.Open();

			await PrepareTable(cnn, columns);

			await ExecuteMigrate(cnn, columns);
		}

		private async Task ExecuteMigrate(ClickHouseConnection cnn, Dictionary<string, ColumnDefine> columns)
		{
			using var bulkCopyInterface = new ClickHouseBulkCopy(cnn)
			{
				DestinationTableName = $"{_options.Database}.{_options.Table}",
				BatchSize = _options.BatchSize
			};

			var columnNames = columns.Select(x => $"`{x.Key}`").ToArray();

			int total = 0;

			while (true)
			{
				var data = FetchRows(_options.BatchSize);
				if (data.Count == 0)
				{
					break;
				}

				var firstStep = total == 0;
				total += data.Count;
				await bulkCopyInterface.WriteToServerAsync(data, columnNames);
				Console.Write(firstStep ? $"{data.Count}" : $", {total}");
				if (data.Count < _options.BatchSize)
				{
					break;
				}
			}
		}

		private void InitializeClickHouseOptions(string[] args)
		{
			var configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.AddCommandLine(args, new Dictionary<string, string>
			{
				{"--host", "Host"},
				{"-h", "Host"},

				{"--port", "Port"},
				{"-port", "Port"},

				{"--user", "User"},
				{"-u", "User"},

				{"--pass", "Password"},
				{"-p", "Password"},

				{"--orderBy", "OrderBy"},
				{"-o", "OrderBy"},

				{"-d", "Database"},
				{"-t", "Table"},

				{"--drop", "DropTable"}
			});
			var configuration = configurationBuilder.Build();
			_options = new ClickHouseOptions(configuration);
		}


		private async Task PrepareTable(ClickHouseConnection cnn, Dictionary<string, ColumnDefine> columns)
		{
			await cnn.ExecuteAsync($"CREATE DATABASE IF NOT EXISTS {_options.Database};");
			if (_options.DropTable)
			{
				Log.Logger.Warning($"DROP table {_options.Database}.{_options.Table}");
				await cnn.ExecuteAsync($"DROP TABLE IF EXISTS {_options.Database}.{_options.Table};");
			}

			await cnn.ExecuteAsync(GenerateCreateTableSql(_options.Database, _options.Table, columns,
				_options.OrderBy));
		}

		protected string GenerateCreateTableSql(string database, string table,
			Dictionary<string, ColumnDefine> columns,
			string[] orderBy)
		{
			var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {database}.{table} (");

			foreach (var column in columns)
			{
				stringBuilder.Append($"`{column.Key}` {ConvertToDataType(column.Value.DataType)}, ");
			}

			stringBuilder.Remove(stringBuilder.Length - 2, 2);

			if (orderBy.Length > 0)
			{
				var orderByPart = string.Join(",", orderBy.Select(x => $"`{x}`"));
				stringBuilder.Append(
					$") ENGINE = MergeTree() ORDER BY ({orderByPart}) SETTINGS index_granularity = 8192");
			}
			else
			{
				stringBuilder.Append($") ENGINE = MergeTree() ORDER BY tuple() SETTINGS index_granularity = 8192");
			}

			var sql = stringBuilder.ToString();
			return sql;
		}

		protected virtual string ConvertToDataType(string type)
		{
			var sizePrefixIndex = type.IndexOf('(');
			var normalTypeName = sizePrefixIndex <= 0 ? type : type.Substring(0, sizePrefixIndex);
			switch (normalTypeName.ToLower())
			{
				case "timestamp":
				case "datetime2":
				case "datetime":
				{
					return "DateTime";
				}
				case "date":
				{
					return "Date";
				}
				case "tinyint":
				{
					return "UInt8";
				}
				case "smallint":
				{
					return "Int16";
				}
				case "int":
				{
					return "Int32";
				}
				case "float":
				{
					return "Float32";
				}
				case "double":
				case "decimal":
				{
					return "Float64";
				}
				case "bigint":
				{
					return "Int64";
				}
				default:
				{
					return "String";
				}
			}
		}
	}
}