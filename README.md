# Moonlight

Moonlight 是一个高性能分布式缓存服务器，由java语言编写，采用多线程架构，实现的功能包括：

- 增删改查操作
- [MDTP通信协议](MDTP.md)
- yaml格式配置文件
- 数据持久化（二进制数据文件）
- 基于Raft算法的集群实现
- LRU缓存更新
- 红黑树索引，跳表索引

## 配置

### 配置文件

路径：`System.getProperty("user.dir") + "/config/application.yml"`

### 配置项

|配置项|说明|
|---|---|
|server.host|主机（暂时没有用）|
|server.port|端口号|
|mode|运行模式（"single"或"cluster"）|
|cluster|集群的相关配置|

## EventBus（事件总线）

### 参与角色

- MdtpSocketServer(接受请求的角色)
- MdtpSocketClient(与集群中其他节点通信的客户端)
- Engine(本地存储引擎)
- BinaryLog(写二进制日志的线程)

### 事件类型

EventBus应该维护三个队列，分别存储以下三种事件：

- 修改本地数据的MDTP请求（例如：set请求，delete请求）
- 不修改本地数据的MDTP请求（例如：get请求）
- 写回给客户端的响应
- 修改系统配置信息的请求（例如：system请求）
- 查看集群相关信息的请求（例如：cluster请求）
- 集群其他节点返回的响应

### 事件生产

MdtpSocketServer生产的事件：
- 修改本地数据的MDTP请求
- 不修改本地数据的MDTP请求
- 修改系统配置信息的请求
- 查看集群相关信息的请求

MdtpSocketClient生产的事件：
- 集群其他节点返回的响应

Engine生产的事件：
- 写回给客户端的响应

### 事件消费

MdtpSocketServer消费的事件：
- 写回给客户端的响应（需要对MdtpResponse进行读操作）

MdtpSocketClient消费的事件：
- 修改本地数据的MDTP请求（需要对MdtpRequest进行读操作）

Engine消费的事件：
- 修改本地数据的MDTP请求
- 不修改本地数据的MDTP请求

BinaryLog消费的事件：
- 修改本地数据的MDTP请求（需要对MdtpRequest进行读操作）


## 线程安全问题

需要解决MdtpRequest和MdtpResponse的线程安全问题。

## 流程

DynamicByteBuffer 申请内存流程： 首先，直接申请给定大小的直接内存，如果申请失败则申请给定大小的 1/2，直到申请到全部内存。如果申请失败，则报异常。

1.0-SNAPSHOT is here: [Moonlight-1.0-SNAPSHOT.tar.gz](https://github.com/ECUST-CST163-ZhangBaiLi/Moonlight/releases/download/1.0-SNAPSHOT/Moonlight-1.0-SNAPSHOT.tar.gz)

## Run moonlight server

Make sure port `7820` is available !

```shell
java -jar server-1.0-SNAPSHOT.jar
```

## Run moonlight client
```shell
java -jar moonlight.client-1.0-SNAPSHOT.jar
```

## `Set`
```shell
Moonlight > set a 30
[OK] Done
```

## `Get`
```shell
Moonlight > get a
[OK] 30
```

## `Update`
```shell
Moonlight > update a 80
[OK] Done
```

## `Delete`
```shell
Moonlight > delete a
[OK] Done
```

## `Exit`
```shell
Moonlight > exit
[Close Connection]
```

