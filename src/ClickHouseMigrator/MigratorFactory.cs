using ClickHouseMigrator.Impl;
using System;
using ClickHouseMigrator.Impl.Excel;

namespace ClickHouseMigrator
{
	public static class MigratorFactory
	{
		public static IMigrator Create(string source)
		{
			source = source?.ToLower();
			switch (source)
			{
				case "mysql":
				{
					return new MySqlMigrator(null);
				}
				case "mssql":
				case "sqlserver":
				{
					return new MsSqlMigrator(null);
				}
				case "excel":
				{
					return new ExcelMigrator();
				}
				default:
				{
					throw new NotImplementedException($"Not implemented {source} migrator.");
				}
			}
		}
	}
}