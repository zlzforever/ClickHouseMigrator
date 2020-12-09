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
ORDER BY KU.TABLE_NAME, KU.ORDINAL_POSITION; ").ToList();

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