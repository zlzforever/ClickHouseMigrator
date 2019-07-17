using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Serilog;

namespace ClickHouseMigrator
{
	static class Program
	{
		static void Main(string[] args)
		{
			if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development")
			{
				args =
					"--lowercase true --source sqlserver --shost localhost --suser sa --spass 1qazZAQ! --sport 1433 --sourced cnblogs --sourcet Cnblogs_Entity_Model -h localhost --targetd cnblogs --targett Cnblogs_Entity_Model --thread 1 -b 2000 --drop true --log false"
						.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
			}

			var options = BuildOptions(args);

			ConfigureLog(options);

			var mode = options.Mode.ToLower();
			if (mode != "parallel" && mode != "sequential")
			{
				Log.Logger.Error("Only support two modes: parallel, sequential.");
				return;
			}

			if (!AppContext.TryGetSwitch("WELCOME", out bool printWelcome) && !printWelcome)
			{
				Console.ForegroundColor = ConsoleColor.Green;
				Console.WriteLine(
					"==========================================================================================================================================");
				Console.WriteLine(
					"======                                    ClickHouse Migrator V1.0.7 MIT   zlzforever@163.com                                      =======");
				Console.WriteLine(@"
--source    or -s    : which RDBMS you want to migrate, right now i implement mysql migrator, for example: mysql
--shost     or -sh   : host of RDBMS, for example: 192.168.90.100, **default value: 127.0.0.1**
--sport     or -sport: port of RDBMS, for example: 3306
--suser     or -su   : user of RDBMS
--spass     or -sp   : password of RDBMS
--host      or -h    : host of Clickhouse for example: 192.168.90.101, **default value: 127.0.0.1**
--port      or -port : port of clickhouse, for example: 9000, **default value: 9000**
--user      or -u    : user of Clickhouse
--pass      or -p    : password of Clickhouse
--thread    or -t    : how many thread use to read data from mysql, **default value: process count of your machine**
--batch     or -b    : how many rows read from mysql one time and submit to clickhouse, **default value: 5000**
--sourced   or -sd   : database of RDBMS
--sourcet   or -st   : table of RDBMS which you want to migrate
--targetd   or -td   : migrate data to which target database in clickhouse, create it if not exists
--targett   or -tt   : migrate data to which target table in clickhouse, create it if not exists
--drop      or -d    : whether drop the exits table in clickhouse before migrating, **default value: false**
--lowercase or -lc   : ignore the word case in clickhouse, **default value: true**
--orderby   or -o    : when order by is null, use primary as order by in clickhouse, if use orderby, then will miss primary
--trace     or -t    : record performance information, **default value: false**
--mode      or -m    : migrate mode, parallel or sequential, when use sequential thread argument are useless, **default value: parallel**
--log       or -l    : whether write file log, **default value: false**
");
				Console.WriteLine(
					"==========================================================================================================================================");
				Console.ForegroundColor = ConsoleColor.White;
				AppContext.SetSwitch("WELCOME", true);
			}

			var start = DateTime.Now;
			var migrator = MigratorFactory.Create(options);
			migrator.Run();
			var end = DateTime.Now;
			Log.Logger.Information($"Complete migrate: {(end - start).TotalSeconds} s.");
		}

		private static Options BuildOptions(string[] args)
		{
			var configurationBuilder = new ConfigurationBuilder();
			configurationBuilder.AddCommandLine(args, new Dictionary<string, string>
			{
				{"--source", "DataSource"},
				{"-s", "DataSource"},

				{"--shost", "SourceHost"},
				{"-sh", "SourceHost"},

				{"--sport", "SourcePort"},
				{"-sport", "SourcePort"},

				{"--suser", "SourceUser"},
				{"-su", "SourceUser"},

				{"--spass", "SourcePassword"},
				{"-sp", "SourcePassword"},

				{"--host", "Host"},
				{"-h", "Host"},

				{"--port", "Port"},
				{"-port", "Port"},

				{"--user", "User"},
				{"-u", "User"},

				{"--pass", "Password"},
				{"-p", "Password"},

				{"--sourced", "SourceDatabase"},
				{"-sd", "SourceDatabase"},

				{"--sourcet", "SourceTable"},
				{"-st", "SourceTable"},

				{"--targetd", "TargetDatabase"},
				{"-td", "TargetDatabase"},

				{"--targett", "TargetTable"},
				{"-tt", "TargetTable"},

				{"--drop", "Drop"},
				{"-d", "Drop"},

				{"--lowercase", "Lowercase"},
				{"-lc", "Lowercase"},

				{"--batch", "Batch"},
				{"-b", "Batch"},

				{"--thread", "Thread"},
				{"-t", "Thread"},

				{"--trace", "Trace"},
				{"-tr", "Trace"},

				{"--mode", "Mode"},
				{"-m", "Mode"},

				{"--log", "Log"},
				{"-l", "Log"},

				{"--orderby", "OrderBy"},
				{"-o", "OrderBy"}
			});
			var configuration = configurationBuilder.Build();
			return new Options(configuration);
		}

		private static void ConfigureLog(Options options)
		{
			var loggerConfiguration = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.Console(theme: SerilogHelper.Theme);
			if (options.Log)
			{
				loggerConfiguration = loggerConfiguration.WriteTo.RollingFile("ClickHouseMigrator.log");
			}

			Log.Logger = loggerConfiguration.CreateLogger();
			Log.Logger.Information($"Options: {string.Join(" ", Environment.GetCommandLineArgs())}");
		}
	}
}