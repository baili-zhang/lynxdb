# Moonlight

一个分布式 KV 存储系统，使用 Java 语言编写，目标是支持超大数据量存储，向着成为业界知名的分布式KV存储系统前进。

## 功能

### 待实现

- Metrics 监控

### 待完善

- Raft 日志压缩

## 运行

Windows 系统下的服务器启动脚本是 start-server.bat ，客户端启动脚本是 start-client.bat。

Linux 系统下的服务端启动脚本是 start-server.sh ，客户端启动脚本是 start-client.sh。

Moonlight 服务器的**默认端口号为`7820`**，确保端口`7820`没有被其他进程占用。

## 执行命令

### CONNECT 命令

```shell
Moonlight> connect 127.0.0.1 7820
INFO: Has connected to [127.0.0.1:7820]
```

### DISCONNECT 命令

```shell
[127.0.0.1:7820] Moonlight> disconnect
INFO: Disconnect from [127.0.0.1:7820]
```

#### SET命令

```shell
[127.0.0.1:7821] Moonlight> set key value
OK
```

#### GET命令

```shell
[127.0.0.1:7821] Moonlight> get key
value
```

#### DELETE命令

```shell
[127.0.0.1:7821] Moonlight> delete key
OK
```

## 配置

### 配置文件

待自定义配置文件格式实现后撰写

### 配置项

待自定义配置文件格式实现后撰写

## 版本

### 1.3-SNAPSHOT

待实现的功能：

- 去掉 yaml 格式的配置文件，自定义配置文件格式
- 实现可动态增加和删除节点的 Raft 集群

### 1.2-SNAPSHOT

实现功能：

- 基于Raft一致性协议的集群实现（需要配置）

### 1.1-SNAPSHOT

实现功能：

- 服务端（MoonlightServer）和客户端（MoonlightClient）
- 插入或更新数据（SET），获取数据（GET），删除数据（DELETE），客户端退出（EXIT）
- 服务端和客户端的通信协议：[MDTP通信协议](MDTP.md)
- YAML格式配置文件
- 数据持久化（二进制日志BinaryLog）
- LRU缓存更新策略

下载地址：[moonlight-1.1-snapshot.tar.gz](https://github.com/ECUST-CST163-ZhangBaiLi/moonlight/releases/download/1.1-SNAPSHOT/moonlight-1.1-snapshot.tar.gz)