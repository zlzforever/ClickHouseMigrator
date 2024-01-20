using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace ClickHouseMigrator
{
    public static class Program
    {
        private static readonly string Line =
            "=======================================================================================================================";

        static async Task Main(string[] args)
        {
            Logger.Information($"Options: {string.Join(" ", Environment.GetCommandLineArgs())}");

            if (!AppContext.TryGetSwitch("WELCOME", out var printWelcome) && !printWelcome)
            {
                var version = typeof(Program).Assembly.GetName().Version;

                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(Line);
                Console.WriteLine(
                    $"ClickHouse Migrator V{version} MIT   zlzforever@163.com");
                Console.WriteLine(@"
--src            : data source: MySql, SqlServer, Excel etc
--src-host       : host of data source, for example: 192.168.90.100, **default value: 127.0.0.1**
--src-port       : port of data source, for example: 3306
--src-user       : user of data source
--src-password   : password of data source
--src-database   : database of data source
--src-table      : table of data source
--host           : Clickhouse host: 192.168.90.101, **default value: 127.0.0.1**
--port           : Clickhouse port, for example: 9000, **default value: 9000**
--user           : Clickhouse user
--password       : Clickhouse password
--database       : Clickhouse database, if this arg is null, will use --src-database as target database name
--table          : Clickhouse table, if this arg is null, will use --src-table as target table name
--thread         : how many thread use to insert data to ClickHouse, **default value: process count of your machine**
--batch          : how many rows insert to ClickHouse one time, **default value: 10000**
--drop-table     : whether drop the exits table in clickhouse before migrating, **default value: false**
--file           : File path of Excel etc
--sheets         : Which sheets will be migrated, columns are same in every sheet, used like: Sheet1,Sheet2,Sheet3
--start-row      : 
--lowercase      : ignore the word case in clickhouse, **default value: true**
");
                Console.WriteLine(Line);
                Console.ForegroundColor = ConsoleColor.White;
                AppContext.SetSwitch("WELCOME", true);
            }

            if (args.Length == 0)
            {
                return;
            }

            if (Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") == "Development" || args.Contains("test"))
            {
                args =
                    "--src mysql --src-host 192.168.192.2 --src-port 3306 --src-user root --src-password 1qazZAQ! --src-database test --src-table user1000w --drop-table true"
                        .Split(new[] { " " }, StringSplitOptions.RemoveEmptyEntries);

                // args =
                // 	"--src excel --file Book.xlsx --database test --table t1 --start-row 2 --sheet-start 2 --drop-table true"
                // 		.Split(new[] {" "}, StringSplitOptions.RemoveEmptyEntries);
            }

            try
            {
                var dataSource = GetDataSource(args);
                var migrator = MigratorFactory.Create(dataSource);
                await migrator.RunAsync(args);
            }
            catch (Exception e)
            {
                Logger.Error(e.Message);
            }
        }

        private static string GetDataSource(string[] args)
        {
            var configurationBuilder = new ConfigurationBuilder();
            configurationBuilder.AddCommandLine(args, new Dictionary<string, string>
            {
                { "--src", "DataSource" }
            });
            var configuration = configurationBuilder.Build();
            return configuration["DataSource"];
        }
    }
}