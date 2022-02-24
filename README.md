# Moonlight

两年前，还在读大学的时候，看了一些Redis的源码，就想用C语言实现一个缓存服务器，项目叫做“RCache”。

然后，写了一个Main函数，就没有继续了，在Github上面留下了一个空仓库。

今年九月的时候，登录这个账号的时候发现了RCache的仓库，想到了以前想实现一个缓存服务的事，就打算用Java实现一个简单的缓存服务器。

为什么没有继续用RCache作为项目的名称？因为RCache这个名称过于干涩，没有意境。

## 功能

### 已实现

- 服务端（MoonlightServer）和客户端（MoonlightClient）
- 插入或更新数据（SET），获取数据（GET），删除数据（DELETE），客户端退出（EXIT）
- 服务端和客户端的通信协议：[MDTP通信协议](MDTP.md)
- YAML格式配置文件
- 数据持久化（二进制日志BinaryLog）
- LRU缓存更新策略

### 待实现

- Metrics监控
- 基于Raft算法的集群实现

### 待完善

- 内存管理：目前插入大量数据可能会导致直接内存溢出，需要增加内存管理
- 二进制日志压缩：二进制日志文件会记录所有更新数据的操作，体积会膨胀的很大，需要压缩

## 运行

Windows系统下的服务器启动脚本是start-server.bat，客户端启动脚本是start-client.bat。

Linux系统下的服务端启动脚本是start-server.sh，客户端启动脚本是start-client.sh。

启动脚本都是简单的一行命令，如需要修改JVM参数，直接修改启动脚本即可。

Moonlight服务器的**默认端口号为7820**，确保端口7820没有被其他进程占用。

### 执行命令

命令格式：命令 键 值

响应格式：[响应码][值的长度][序列标识][值]

#### SET命令

```shell
Moonlight> set key1 value1
[SUCCESS_NO_VALUE][0][1][]
```

#### GET命令

```shell
Moonlight> get key1
[VALUE_EXIST][6][2][value1]
```

#### DELETE命令

```shell
Moonlight> delete key1
[SUCCESS_NO_VALUE][0][3][]
```

## 配置

### 配置文件

路径：`System.getProperty("user.dir") + "/config/application.yml"`

### 配置项

配置项中的`.`格式对应YAML中的关系如下，例如`server.host`:

```yaml
server:
  host: "xxx.xxx.xxx.xxx"
```

|配置项|说明|
|---|---|
|server.host|主机|
|server.port|端口号|
|server.backlog|最大连接数|
|server.io_thread_core_pool_size|IO线程池的核心线程数|
|server.io_thread_max_pool_size|IO线程池的最大线程数|
|server.io_thread_keep_alive_time|IO线程池的非核心线程的存活时间|
|server.io_thread_blocking_queue_size|IO线程池的阻塞队列大小|
|sync_write_log|是否同步写二进制日志|
|cache.capacity|cache的最大容量|
|mode|运行模式（"single"或"cluster"）|
|cluster|集群的相关配置|
|cluster.nodes|集群的节点信息|

## 版本

### 1.2-SNAPSHOT

实现功能：

- 实现PING命令
- 心跳检测
- 主从复制
- 日志复制
- 定义传输协议的scheme，并根据scheme的格式来读取数据和格式化数据

### 1.1-SNAPSHOT

实现功能：

- 服务端（MoonlightServer）和客户端（MoonlightClient）
- 插入或更新数据（SET），获取数据（GET），删除数据（DELETE），客户端退出（EXIT）
- 服务端和客户端的通信协议：[MDTP通信协议](MDTP.md)
- YAML格式配置文件
- 数据持久化（二进制日志BinaryLog）
- LRU缓存更新策略

下载地址：[moonlight-1.1-snapshot.tar.gz](https://github.com/ECUST-CST163-ZhangBaiLi/moonlight/releases/download/1.1-SNAPSHOT/moonlight-1.1-snapshot.tar.gz)

### 1.0-SNAPSHOT

实现功能：

- 客户端与服务端之间通信
- SET，GET，DELETE，UPDATE，EXIT

下载地址: [Moonlight-1.0-SNAPSHOT.tar.gz](https://github.com/ECUST-CST163-ZhangBaiLi/Moonlight/releases/download/1.0-SNAPSHOT/Moonlight-1.0-SNAPSHOT.tar.gz)
