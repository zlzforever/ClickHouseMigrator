using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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

		protected override Lazy<string> ConnectionString => new Lazy<string>(
			$"Data Source={Options.SourceHost};Database='{Options.SourceDatabase}';User ID={Options.SourceUser}; Password={Options.SourcePassword};Port={Options.SourcePort};SslMode=None;Connection Timeout=120");

		protected override string ConvertToClickHouseDataType(string type)
		{
			var sizePrefixIndex = type.IndexOf('(');
			var normalTypeName = sizePrefixIndex <= 0 ? type : type.Substring(0, sizePrefixIndex);
			switch (normalTypeName)
			{
				case "timestamp":
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

		protected override IDbConnection CreateDbConnection()
		{
			var conn = new MySqlConnection(ConnectionString.Value);
			return conn;
		}

		protected override string GetSelectAllSql()
		{
			return $"SELECT * FROM `{Options.SourceDatabase}`.`{Options.SourceTable}`";
		}
	}
}