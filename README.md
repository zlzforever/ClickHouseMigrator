# ClickHouseMigrator

Use to migrate data from RDBMS to ClickHouse, create database and table auto.

### DEVELOP ENVIROMENT

- Visual Studio 2017(15.3 or later)
- [.NET Core 2.1 or later](https://www.microsoft.com/net/download/windows)

### OPTIONS

+ --source    or -s    : which RDBMS you want to migrate, right now i implement mysql migrator, for example: mysql
+ --shost     or -sh   : host of RDBMS, for example: 192.168.90.100, **default value: 127.0.0.1**
+ --sport     or -sport: port of RDBMS, for example: 3306
+ --suser     or -su   : user of RDBMS
+ --spass     or -sp   : password of RDBMS
+ --host      or -h    : host of Clickhouse for example: 192.168.90.101, **default value: 127.0.0.1**
+ --port      or -port : port of clickhouse, for example: 9000, **default value: 9000**
+ --user      or -u    : user of Clickhouse
+ --pass      or -p    : password of Clickhouse
+ --thread    or -t    : how many thread use to read data from mysql, **default value: process count of your machine**
+ --batch     or -b    : how many rows read from mysql one time and submit to clickhouse, **default value: 5000**
+ --sourced   or -sd   : database of RDBMS
+ --sourcet   or -st   : table of RDBMS which you want to migrate
+ --targetd   or -td   : migrate data to which target database in clickhouse, create it if not exists
+ --targett   or -tt   : migrate data to which target table in clickhouse, create it if not exists
+ --drop      or -d    : whether drop the exits table in clickhouse before migrating, **default value: false**
+ --lowercase or -lc   : ignore the word case in clickhouse, **default value: true**
+ --orderby   or -o    : when order by is null, use primary as order by in clickhouse, if use orderby, then will miss primary
+ --trace     or -t    : record performance information, **default value: false**
+ --mode      or -m    : migrate mode, parallel or sequential, when use sequential thread argument are useless, **default value: parallel**
+ --log       or -l    : whether write file log, **default value: false**

### HOW TO USE

- install dotnet core 2.2 follow: https://www.microsoft.com/net/learn/get-started/windows#install
- on windows run below command in command prompt, and in terminal for linux

        dotnet tool install -g ClickHouseMigrator

- the migrate tool named chm, so run tool like below

      > chm --source sqlserver --shost localhost --suser sa --spass 1qazZAQ! --sport 1433 \
      --sourcedb cnblogs --sourcetb cnblogs_entity_model \
      -h localhost \
      --targetdb cnblogs \
      --targettb cnblogs_entity_model \
	  --thread 1 \
	  -b 2000 \
	  --drop true \
	  --log true \
	  -m parallel


### SHOW

* 32  Intel(R) Xeon(R) CPU E5-2620 v4 @ 2.10GHz
* 128 G
* 4T HDD * 4 RAID10 IOPS 3000

![DESIGN](https://github.com/zlzforever/ClickHouseMigrator/raw/master/images/example.png?raw=true)

### Buy me a coffe

![](https://github.com/zlzforever/DotnetSpiderPictures/raw/master/pay.png)

### AREAS FOR IMPROVEMENTS

Email: zlzforever@163.com
