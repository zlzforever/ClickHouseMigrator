using CommandLine;
using System;
using System.Collections.Generic;

namespace ClickHouseMigrator
{
	public class Options
	{
		[Option('s', "source", Required = false, HelpText = "Source database.")]
		public string Source { get; set; } = "mysql";

		[Option("shost", Required = false, HelpText = "Source database host.")]
		public string SourceHost { get; set; }

		[Option("sport", Required = false, HelpText = "Source database port.")]
		public int SourcePort { get; set; }

		[Option("suser", Required = false, HelpText = "Source database user.")]
		public string SourceUser { get; set; }

		[Option("spass", Required = false, HelpText = "Source database password.")]
		public string SourcePassword { get; set; }

		[Option('h', "host", Required = false, HelpText = "Clickhouse host.")]
		public string Host { get; set; } = "127.0.0.1";

		[Option("port", Required = false, HelpText = "Clickhouse port.")]
		public int Port { get; set; } = 9000;

		[Option('u', "user", Required = false, HelpText = "Clickhouse user.")]
		public string User { get; set; } = "default";

		[Option('p', "password", Required = false, HelpText = "Clickhouse password.")]
		public string Password { get; set; }

		[Option("sourcedb", Required = true, HelpText = "Source database to migrate.")]
		public string SourceDatabase { get; set; }

		[Option("sourcetb", Required = true, HelpText = "Source table to migrate.")]
		public string SourceTable { get; set; }

		[Option("targetdb", Required = false, HelpText = "Target database migrate to.")]
		public string TargetDatabase { get; set; }

		[Option("targettb", Required = false, HelpText = "Target table migrate to.")]
		public string TargetTable { get; set; }

		[Option("drop", Required = false, HelpText = "If drop target table if exists.")]
		public bool Drop { get; set; }

		[Option("ignorecase", Required = false, HelpText = "Ignore database, table name, column name case.")]
		public bool IgnoreCase { get; set; } = true;

		[Option('b', "batch", Required = false, HelpText = "Submit count of a batch.")]
		public int Batch { get; set; } = 5000;

		[Option("thread", Required = false, HelpText = "Thread count.")]
		public int Thread { get; set; } = Environment.ProcessorCount;

		[Option("performance", Required = false, HelpText = "Record performance.")]
		public bool TracePerformance { get; set; } = false;

		[Option('m', "mode", Required = false, HelpText = "Migrate mode: parallel or sequential, when use sequential thread and batch are useless.")]
		public string Mode { get; set; } = "parallel";

		[Option("log", Required = false, HelpText = "Write file log.")]
		public bool Log { get; set; } = false;

		[Option("orderby", Required = false, HelpText = "Order by for clickhouse.", Separator = ',')]
		public IEnumerable<string> OrderBy { get; set; }

		public string GetTargetDatabase()
		{
			return IgnoreCase ? TargetDatabase.ToLowerInvariant() : TargetDatabase;
		}

		public string GetTargetTable()
		{
			return IgnoreCase ? TargetTable.ToLowerInvariant() : TargetTable;
		}
	}
}
