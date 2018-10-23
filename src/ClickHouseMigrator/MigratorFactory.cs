using ClickHouseMigrator.Impl;
using System;

namespace ClickHouseMigrator
{
	public static class MigratorFactory
	{
		public static IMigrator Create(Options arguments)
		{
			var source = arguments.Source.ToLower();
			switch (source)
			{
				case "mysql":
					{
						return new MySqlMigrator(arguments);
					}
				case "mssql":
					{						
						return new MsSqlMigrator(arguments);
					}
				default:
					{
						throw new NotImplementedException($"Not impemented {source} migrator.");
					}
			}
		}
	}
}
