# LynxDB 数据库（Version 1.0）

LynxDB 支持（列族，列，键，值）的表结构存储，其中列族和列必须为字符串，键和值为 byte[]。LynxDB 支持插入，查询，删除和范围查找等数据操作，目前不支持事务。

## 配置文件

默认配置文件：`./lynxdb/config/app.cfg`

```text
host            = 127.0.0.1
port            = 7820
messagePort     = 7263
dataDir         = [base]/data/single/base
timeoutDir      = [base]/data/single/timeout
runningMode     = single
```

配置项说明：

| 配置项         | 描述      |
|-------------|---------|
| host        | ip 或域名  |
| port        | 端口号     |
| messagePort | 消息端口    |
| dataDir     | 数据目录    |
| timeoutDir  | 超时的数据目录 |
| runningMode | 运行模式    |

## 数据操作

### Insert 命令

插入数据：

```shell
insert [key] [columnFamily] [column] [value]
```

### Find 命令

查询单个 column：

```shell
find [key] [columnFamily] [column]
```

查询多个 column：

```shell
find [key] [columnFamily]
```

### Delete 命令

删除数据：

```shell
delete [key] [columnFamily] [column]
```

### Exist 命令

查询 Key 是否存在：

```shell
exist [key] [columnFamily] [column]
```

### Range Next 命令

从 beginKey 往后范围查找：

```shell
range-next [columnFamily] [mainColumn] [beginKey] [limit]
```

### Range Before 命令

从 endKey 往前范围查找：

```shell
range-before [columnFamily] [mainColumn] [endKey] [limit]
```