using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Dapper;

namespace ClickHouseMigrator.SqlServer
{
	public class SqlServerMigrator : RDBMSMigrator
	{
		protected override List<ColumnDefine> FetchTablesColumns()
		{
			using var conn = CreateDbConnection();
			var primaryKeys = conn.Query($@"
					SELECT column_name as PRIMARYKEYCOLUMN
					FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
					INNER JOIN
						INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
							  ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
								 TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
								 KU.table_name = '{Options.SourceTable}'
					ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION;"
				).ToList();

			var columns = conn
				.Query(
					$"SELECT * FROM [{Options.SourceDatabase}].INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{Options.SourceTable}';")
				.ToList();
			var columnDefines = new List<ColumnDefine>();
			foreach (IDictionary<string, dynamic> column in columns)
			{
				var columnDefine = new ColumnDefine
				{
					DataType = column["DATA_TYPE"],
					IsPrimary = primaryKeys.Select(f => f.PRIMARYKEYCOLUMN).ToList()
						.Contains(column["COLUMN_NAME"]),
					Name = column["COLUMN_NAME"]
				};
				columnDefines.Add(columnDefine);
			}

			return columnDefines;
		}

		protected override Lazy<string> ConnectionString => new Lazy<string>(() =>
			$"Server=tcp:{Options.SourceHost},{Options.SourcePort};Initial Catalog={Options.SourceDatabase};Password={Options.SourcePassword};Persist Security Info=False;User ID={Options.SourceUser};MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Connection Timeout=3000;");

		protected override string ConvertToClickHouseDataType(string type)
		{
			var sizePrefixIndex = type.IndexOf('(');
			var normalTypeName = sizePrefixIndex <= 0 ? type : type.Substring(0, sizePrefixIndex);
			switch (normalTypeName.ToLower())
			{
				case "timestamp":
				case "smalldatetime":
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
				case "money":
					{
						return "Decimal64(4)";
					}
				case "smallmoney":
					{
						return "Decimal32(4)";
					}
				case "bit":
					{
						// Clickhouse has no Boolean
						return "UInt8";
					}
				case "float":
					{
						return "Float64";
					}
				case "real":
					{
						return "Float32";
					}
				case "numeric":
				case "decimal":
					{
						//todo get scale & precision as Float is incorrect because it is an approximation!
						return "Float64";
					}
				case "bigint":
					{
						return "Int64";
					}
				case "uniqueidentifier":
					{
						return "UUID";
					}
				//case "char"/nchar:
				//	{
				//      TODO: must receive size
				//		return "FixedString(n)
				//	}
				default:
					{
						return "String";
					}
			}
		}

		protected override IDbConnection CreateDbConnection()
		{
			var conn = new SqlConnection(ConnectionString.Value);
			return conn;
		}

		protected override string GetSelectAllSql()
		{
			return $"SELECT * FROM [{Options.SourceTable}];";
		}
	}
}