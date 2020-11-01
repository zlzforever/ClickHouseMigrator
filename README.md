# ClickHouseMigrator

Use to migrate data from RDBMS to ClickHouse, create database and table auto.

### DEVELOP ENVIRONMENT

- Visual Studio 2017(15.3 or later)
- [.NET Core 3.1 or later](https://www.microsoft.com/net/download/windows)

### OPTIONS

```
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
```

### HOW TO USE

- install dotnet core 3.1 follow: https://www.microsoft.com/net/learn/get-started/windows#install
- on windows run below command in command prompt, and in terminal for linux

        dotnet tool install -g ClickHouseMigrator

- the migrate tool named chm, so run tool like below

      > chm --src mysql --src-host localhost --src-port 3306 --src-user root --src-password 1qazZAQ! \
        --src-database test --src-table user1000w --drop-table true


### SHOW

* 3.2 GHz 8-Core Intel Xeon W
* 32 G
* 1T SSD

#### ClickHouse command

```
CREATE TABLE test.user
ENGINE = MergeTree
ORDER BY id AS
SELECT *
FROM mysql('192.168.192.2:3306', 'test', 'user1000w', 'root', '1qazZAQ!')
```

Elapsed: 27.965 sec. Processed 18.68 million rows, 895.49 MB (667.94 thousand rows/s., 32.02 MB/s.)

#### ClickHouseMigrator

![](https://github.com/zlzforever/ClickHouseMigrator/blob/master/images/example.png)

Elapsed 63 sec. Processed 18627236 rows (295670 rows/s.)

### Buy me a coffee

![](https://github.com/zlzforever/ClickHouseMigrator/blob/master/images/alipay.jpeg)

### AREAS FOR IMPROVEMENTS

Email: zlzforever@163.com
