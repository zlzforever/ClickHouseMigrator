using System;

namespace ClickHouseMigrator
{
	public static class Logger
	{
		private static object Locker = new object();

		public static void Information(string msg)
		{
			lock (Locker)
			{
				Console.ForegroundColor = ConsoleColor.White;
				Console.Write("INFO: ");
				Console.WriteLine(msg);
			}
		}

		public static void Error(string msg)
		{
			lock (Locker)
			{
				Console.ForegroundColor = ConsoleColor.DarkRed;
				Console.Write("ERROR: ");
				Console.ForegroundColor = ConsoleColor.White;
				Console.WriteLine(msg);
			}
		}

		public static void Warning(string msg)
		{
			lock (Locker)
			{
				Console.ForegroundColor = ConsoleColor.Yellow;
				Console.Write("WARN: ");
				Console.ForegroundColor = ConsoleColor.White;
				Console.WriteLine(msg);
			}
		}
	}
}