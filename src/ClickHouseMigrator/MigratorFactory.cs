using System;
using ClickHouseMigrator.Excel;
using ClickHouseMigrator.MySql;
using ClickHouseMigrator.SqlServer;

namespace ClickHouseMigrator
{
	public static class MigratorFactory
	{
		public static IMigrator Create(string source)
		{
			source = source?.ToLower();
			if (string.IsNullOrWhiteSpace(source))
			{
				throw new Exception("DataSource is required");
			}

			switch (source)
			{
				case "mysql":
				{
					return new MySqlMigrator();
				}
				case "mssql":
				case "sqlserver":
				{
					return new SqlServerMigrator();
				}
				case "excel":
				{
					return new ExcelMigrator();
				}
				default:
				{
					throw new Exception($"DataSource: {source} is not supported");
				}
			}
		}
	}
}