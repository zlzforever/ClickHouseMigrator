# ClickHouseMigrator

Use to migrate data from RDBMS to ClickHouse, create database and table auto.

### DEVELOP ENVIROMENT

- Visual Studio 2017(15.3 or later)
- [.NET Core 2.1 or later](https://www.microsoft.com/net/download/windows)

### OPTIONS

+ --source: which RDBMS you want to migrate, right now i implement mysql migrator, for example: mysql
+ --shost： host of RDBMS, for example: 192.168.90.100, **default value: 127.0.0.1**
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

### HOW TO USE

- install dotnet core 2.1 follow: https://www.microsoft.com/net/learn/get-started/windows#install
- on windows run below command in command prompt, and in terminal for linux

        dotnet tool install ClickHouseMigrator

- the migrate tool named chm, so run tool like below

      > chm --shost 192.168.90.100 --suser user --spass xxxxxxxx --sport 53306
				--sourcedb jd --sourcetb sku_sold_2018_05_21
				-h 192.168.90.101 -u default -p UjzBOxCL
				--targetdb jd --targettb sku_sold
				--thread 4 -b 1000
				--drop true --log true -m parallel


### SHOW

* 32  Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz
* 128 G
* 4T HDD * 4 RAID10 IOPS 3000

![DESIGN](https://github.com/zlzforever/ClickHouseMigrator/raw/master/images/example.png?raw=true)

### Buy me a coffe

![](https://github.com/zlzforever/DotnetSpiderPictures/raw/master/pay.png)

### AREAS FOR IMPROVEMENTS

Email: zlzforever@163.com
