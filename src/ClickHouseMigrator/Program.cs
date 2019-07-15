using System;
using CommandLine;
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
					"--source sqlserver --shost localhost --suser sa --spass 1qazZAQ! --sport 1433 --sourcedb cnblogs --sourcetb cnblogs_entity_model -h localhost --targetdb cnblogs --targettb cnblogs_entity_model --thread 1 -b 2000 --drop true --log true"
						.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
			}

			Parser.Default.ParseArguments<Options>(args).WithParsed(a =>
			{
				ConfigureLog(a);

				var mode = a.Mode.ToLower();
				if (mode != "parallel" && mode != "sequential")
				{
					Log.Logger.Error("Only support two modes: parallel, sequential.");
					return;
				}

				if (!AppContext.TryGetSwitch("WELCOME", out bool printWelcome) && !printWelcome)
				{
					Console.ForegroundColor = ConsoleColor.Green;
					Console.WriteLine(
						"=============================================================================================");
					Console.WriteLine(
						"======                ClickHouse Migrator V1.0.7 MIT   zlzforever@163.com             =======");
					Console.WriteLine(
						"=============================================================================================");
					Console.ForegroundColor = ConsoleColor.White;
					AppContext.SetSwitch("WELCOME", true);
				}

				if (string.IsNullOrWhiteSpace(a.TargetDatabase))
				{
					a.TargetDatabase = a.SourceDatabase;
				}

				if (string.IsNullOrWhiteSpace(a.TargetTable))
				{
					a.TargetTable = a.SourceTable;
				}

				if (string.IsNullOrWhiteSpace(a.SourceHost))
				{
					a.SourceHost = "127.0.0.1";
				}

				if (string.IsNullOrWhiteSpace(a.User))
				{
					a.User = "default";
				}

				if (a.Batch < 1000)
				{
					a.Batch = 1000;
					Log.Logger.Warning("Batch should not less than 1000.");
				}

				//preventing SQL Exception about "The server supports a maximum of 2000 parameters"
				if (a.Source == "mssql" && a.Batch > 2000)
				{
					a.Batch = 2000;
					Log.Logger.Warning("Unfortunately on MsSQL server Batch size  should not greater than 2000.");
				}

				var start = DateTime.Now;
				var migrator = MigratorFactory.Create(a);
				migrator.Run();
				var end = DateTime.Now;
				Log.Logger.Information($"Complete migrate: {(end - start).TotalSeconds} s.");
			}).WithNotParsed(errors => { });
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