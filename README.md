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

## 架构

### 参与角色

- EventBus(事件总线)
- MdtpSocketServer(MDTP服务器，单线程)
- BinaryLogWriter(二进制日志的线程，单线程)
- SimpleCache(简单的存储引擎，单线程)
- MdtpSocketClient(MDTP客户端，与集群的节点通信，单线程)
- ResponseOrganizer(集群响应组织器，单线程)

### 事件

#### 线程安全问题

1. 基于线程安全的考虑，只能在EventBus线程中修改Event对象的属性，其他线程不能修改Event对象的属性。
2. Event对象的值的任何修改操作都必须加锁，因为一个Event对象可能会分发给多个线程，不加锁会导致线程安全问题。

#### 事件类型

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

## 主线程工作

- 初始化EventBus
- 初始化各种Executor
- 将各种Executor注册进EventBus
- 启动Dispatcher
- 启动各种Executor（按照依赖关系）
- 恢复本地数据（读二进制日志文件）

### Executor的种类

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

