using System;
using System.Collections.Generic;
using CommandLine;
using Serilog;
using Serilog.Sinks.SystemConsole.Themes;

namespace ClickHouseMigrator
{
	static class Program
	{
		static void Main(string[] args)
		{
			Parser.Default.ParseArguments<Options>(args).WithParsed(a =>
			{
				var loggerConfiguration = new LoggerConfiguration()
				.MinimumLevel.Verbose()
				.WriteTo.Console(theme: Theme);
				if (a.Log)
				{
					loggerConfiguration = loggerConfiguration.WriteTo.RollingFile("ClickHouseMigrator.log");
				}

				Log.Logger = loggerConfiguration.CreateLogger();
				Log.Logger.Information($"Options: {string.Join(" ", args)}");
				var mode = a.Mode.ToLower();
				if (mode != "parallel" && mode != "sequential")
				{
					Log.Logger.Error("Only support two mode: parallel, sequential.");
					return;
				}
				if (AppContext.GetData("WELCOME") == null)
				{
					Console.ForegroundColor = ConsoleColor.Green;
					Console.WriteLine("=============================================================================================");
					Console.WriteLine("======                ClickHouse Migrator V1.0 MIT   zlzforever@163.com               =======");
					Console.WriteLine("=============================================================================================");
					Console.ForegroundColor = ConsoleColor.White;
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
				var start = DateTime.Now;
				var migrator = MigratorFactory.Create(a);
				migrator.Run();
				var end = DateTime.Now;
				Log.Logger.Information($"Complete migrate: {(end - start).TotalSeconds} s.");
			}).WithNotParsed(errors =>
			{
			});
		}

		private static AnsiConsoleTheme Theme { get; } = new AnsiConsoleTheme(
			new Dictionary<ConsoleThemeStyle, string>
			{
				[ConsoleThemeStyle.Text] = "\x1b[38;5;0253m",
				[ConsoleThemeStyle.SecondaryText] = "\x1b[38;5;0246m",
				[ConsoleThemeStyle.TertiaryText] = "\x1b[38;5;0242m",
				[ConsoleThemeStyle.Invalid] = "\x1b[33;1m",
				[ConsoleThemeStyle.Null] = "\x1b[38;5;0038m",
				[ConsoleThemeStyle.Name] = "\x1b[38;5;0081m",
				[ConsoleThemeStyle.String] = "\x1b[38;5;0216m",
				[ConsoleThemeStyle.Number] = "\x1b[38;5;151m",
				[ConsoleThemeStyle.Boolean] = "\x1b[38;5;0038m",
				[ConsoleThemeStyle.Scalar] = "\x1b[38;5;0079m",
				[ConsoleThemeStyle.LevelVerbose] = "\x1b[32m",
				[ConsoleThemeStyle.LevelDebug] = "\x1b[37m",
				[ConsoleThemeStyle.LevelInformation] = "\x1b[37;1m",
				[ConsoleThemeStyle.LevelWarning] = "\x1b[38;5;0229m",
				[ConsoleThemeStyle.LevelError] = "\x1b[38;5;0197m\x1b[48;5;0238m",
				[ConsoleThemeStyle.LevelFatal] = "\x1b[38;5;0197m\x1b[48;5;0238m"
			});
	}
}
