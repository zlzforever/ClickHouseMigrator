# ClickHouseMigrator

Use to migrate data from RDBMS to ClickHouse. Create database and table auto.

### DEVELOP ENVIROMENT

- Visual Studio 2017(15.3 or later)
- [.NET Core 2.1 or later](https://www.microsoft.com/net/download/windows)

### BASE USAGE

+ --source:  which RDBMS you want to migrate, right now i implement mysql migrator, for example: mysql
+ --shost： host of RDBMS, for example: 192.168.90.100
+ --sport: port of RDBMS, for example: 3306
+ --suser: user of RDBMS
+ --spass: password of RDBMS
+ -h or --host: host of Clickhouse for example: 192.168.90.101, **default value: 127.0.0.1**
+ --port: port of clickhouse, for example: 9000, **default value: 9000**
+ -u or --user: user of Clickhouse
+ -p or --password password of Clickhouse
+ --thread: how many thread use to read data from mysql, **default value: process count of your machine**
+ -b or --batch: how many rows read from mysql one time and submit to clickhouse, **default value: 5000**
+ --sourcedb: database of RDBMS
+ --sourcetb: table of RDBMS which you want to migrate
+ --targetdb: migrate data to which target database in clickhouse, create it if not exists
+ --targettb: migrate data to which target table in clickhouse, create it if not exists
+ --drop: whether drop the exits table in clickhouse before migrating, **default value: false**
+ --ignorecase: ignore the word case in clickhouse, **default value: true**
+ --mdate: the additional column record migrate date, help create correct clickhouse table, **default value: migrate_date**
+ --performance: record performance information, **default value: false**
+ -m or --mode: migrate mode, parallel or sequential, when use sequential thread argument are useless, **default value: parallel**
+ --log: whether write file log, **default value: false**

         # ClickHouseMigrator -s mysql
					--shost 192.168.90.100 --suser user --spass xxxxxxxx --sport 53306
					-h 192.168.90.101 -u default -p UjzBOxCL
					--thread 4 -b 1000
					--sourcedb jd
					--sourcetb sku_sold_2018_05_21
					--targetdb jd
					--targettb sku_sold
					--drop true

### Buy me a coffe

![](https://github.com/zlzforever/DotnetSpiderPictures/raw/master/pay.png)

### AREAS FOR IMPROVEMENTS

Email: zlzforever@163.com
