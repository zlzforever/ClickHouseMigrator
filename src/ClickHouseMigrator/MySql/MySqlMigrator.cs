using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using Dapper;
using MySqlConnector;

namespace ClickHouseMigrator.MySql
{
	public class MySqlMigrator : RDBMSMigrator
	{
		protected override List<ColumnDefine> FetchTablesColumns()
		{
			using var conn = CreateDbConnection();
			var columns = conn.Query($"show columns from `{Options.SourceDatabase}`.`{Options.SourceTable}`;")
				.ToList();
			var columnDefines = new List<ColumnDefine>();
			foreach (IDictionary<string, dynamic> column in columns)
			{
				var columnDefine = new ColumnDefine
				{
					DataType = ConvertToClickHouseDataType(column["Type"]),
					IsPrimary = column["Key"] == "PRI",
					Name = column["Field"]
				};
				columnDefines.Add(columnDefine);
			}

			return columnDefines;
		}

		protected override string ConvertToClickHouseDataType(string type)
		{
			var normalTypeName =
				Regex.Replace(type, @"[0-9()]+",
					"");
			return normalTypeName switch
			{
				"tinyint unsigned" => "UInt8",
				"smallint unsigned" => "UInt16",
				"int unsigned" => "UInt32",
				"mediumint unsigned" => "UInt32",
				"datetime" => "DateTime",
				"timestamp" => "DateTime",
				"date" => "Date",
				"tinyint" => "UInt8",
				"smallint" => "Int16",
				"int" => "Int32",
				"mediumint" => "Int32",
				"bigint unsigned" => "UInt64",
				"float" => "Float32",
				"double" => "Float64",
				"decimal" => "Float64",
				"bigint" => "Int64",
				_ => "String"
			};
		}

		protected override IDbConnection CreateDbConnection()
		{
			var conn = new MySqlConnection(
				$"Data Source={Options.SourceHost};Database='{Options.SourceDatabase}';User ID={Options.SourceUser}; Password={Options.SourcePassword};Port={Options.SourcePort};SslMode=None;Connection Timeout=120");
			return conn;
		}

		protected override string GetSelectAllSql()
		{
			return $"SELECT * FROM `{Options.SourceDatabase}`.`{Options.SourceTable}`";
		}
	}
}