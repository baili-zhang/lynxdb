# LynxDB

LynxDB is a light-weight distributed storage system implemented in the Java language and based on the Raft protocol. The bottom layer of LynxDb uses the RocksDB storage engine. LynxDB supports KV storage and table structure storage.

## Running LynxDB

The server startup script under Windows system is `start-server.bat` , and the client startup script is `start-client.bat`.

The server startup script under Linux system is `start-server.sh` , and the client startup script is `start-client.sh`.

The default port number for LynxDB server is `7820`, make sure port `7820` is not occupied by other processes.

## Query Language

LQL (LynxDB Query Language) is a simple query statement similar to SQL statement, including create, delete, query, insert data and other statements.

**CREATE**

*Create table*

```
CREATE TABLE `user_table`;
```

*Create kv store*

```
CREATE KVSTORE `user_kv`;
```

*create table column*

```
CREATE COLUMNS `name`, `age` in `user_table`;
```

Only tables support columns, the KV store does not.

**SHOW**

*Show all tables*

```
SHOW TABLES;
```

*Show all kv stores*

```
SHOW KVSTORES;
```

*Show all columns in table*

```
SHOW COLUMNS IN `user_table`;
```

**DROP**

*Drop table*

```
DROP TABLE `user_table`;
```

*Drop kv store*

```
DROP KVSTORE `user_kv`;
```

*Drop columns of table*

```
DROP COLUMNS `name`, `age` IN `user_table`;
```

**SELECT**

*Select data from table*

```
SELECT `name`, `age`
    FROM TABLE `user_table`
    WHERE KEY IN `NO.1`, `NO.2`;
```

*Select data from kv store*

```
SELECT FROM KVSTORE `count_kv`
    WHERE KEY IN `article_count`, `user_count`;
```

**INSERT**

*Insert data into table*

```
INSERT INTO TABLE `user_table`
      (`name`,`age`)
      VALUES
          (`NO.1`, `Kobe`, `31`),
          (`NO.2`, `Trump`, `63`);
```

*Insert data into kv store*

```
INSERT INTO KVSTORE `count_kv`
    VALUES
        (`article_count`,`300`),
        (`user_count`,`20`);
```

**DELETE**

*Delete data from table*

```
DELETE `NO.1`, `NO.2` FROM TABLE `user_table`;
```

*Delete data from kv store*

```
DELETE `article_count`,`user_count` FROM KVSTORE `count_kv`;
```

## Configuration

**Configuration file**

Dir: `/config/app.cfg`

**Configuration item**

```
host = 127.0.0.1
port = 7820
```

## Maintainers

See [MAINTAINERS](./MAINTAINERS)