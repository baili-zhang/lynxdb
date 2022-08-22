# Moonlight

Moonlight 是一款基于 Raft 协议的、轻量级的、使用 Java 语言实现的分布式存储系统，底层采用 RocksDB 存储引擎，支持 KV 存储和表结构存储。

## 运行

Windows 系统下的服务器启动脚本是 start-server.bat ，客户端启动脚本是 start-client.bat。

Linux 系统下的服务端启动脚本是 start-server.sh ，客户端启动脚本是 start-client.sh。

Moonlight 服务器的**默认端口号为`7820`**，确保端口`7820`没有被其他进程占用。

## 开始使用

### MQL 查询语句

MQL (Moonlight Query Language) 是一种与 SQL 语句类似的简单查询语句，包括创建，删除，查询，插入数据等语句。

**CREATE 语句**

*创建表*

```
CREATE TABLE `user_table`;
```

*创建 KV 库*

```
CREATE KVSTORE `user_kv`;
```

*创建列*

```
CREATE COLUMNS `name`, `age` in `user_table`;
```

只有表支持列，KV 库不支持列。

**SHOW 语句**

*查看所有表*

```
SHOW TABLES;
```

*查看所有 KV 库*

```
SHOW KVSTORES;
```

*查看表的所有列*

```
SHOW COLUMNS IN `user_table`;
```

**DROP 语句**

*删除表*

```
DROP TABLE `user_table`;
```

*删除 KV 库*

```
DROP KVSTORE `user_kv`;
```

*删除表的列*

```
DROP COLUMNS `name`, `age` IN `user_table`;
```

**SELECT 语句**

*查询表中的数据*

```
SELECT `name`, `age`
    FROM TABLE `user_table`
    WHERE KEY IN `NO.1`, `NO.2`;
```

*查询 KV 库中的数据*

```
SELECT FROM KVSTORE `count_kv`
    WHERE KEY IN `article_count`, `user_count`;
```

**INSERT 语句**

*将数据插入表中*

```
INSERT INTO TABLE `user_table`
      (`name`,`age`)
      VALUES
          (`NO.1`, `Kobe`, `31`),
          (`NO.2`, `Trump`, `63`);
```

*将数据插入 KV 库中*

```
INSERT INTO KVSTORE `count_kv`
    VALUES
        (`article_count`,`300`),
        (`user_count`,`20`);
```

**DELETE 语句**

*从表中删除数据*

```
DELETE `NO.1`, `NO.2` FROM TABLE `user_table`;
```

*从 KV 库中删除数据*

```
DELETE `article_count`,`user_count` FROM KVSTORE `count_kv`;
```

## 配置

### 配置文件

目录：`/config/app.cfg`

### 配置项

```
host = 127.0.0.1
port = 7820
```

## 维护人

[维护人](./MAINTAINERS)