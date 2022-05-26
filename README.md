# Moonlight

Moonlight 是一款基于 Raft 协议的、轻量级的、使用 Java 语言实现的分布式 KV 存储系统。支持 `GET`、`SET`、`DELETE` 的简单 KV 操作。

## 运行

Windows 系统下的服务器启动脚本是 start-server.bat ，客户端启动脚本是 start-client.bat。

Linux 系统下的服务端启动脚本是 start-server.sh ，客户端启动脚本是 start-client.sh。

Moonlight 服务器的**默认端口号为`7820`**，确保端口`7820`没有被其他进程占用。

## 开始使用

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

### SET命令

```shell
[127.0.0.1:7821] Moonlight> set key value
OK
```

### GET命令

```shell
[127.0.0.1:7821] Moonlight> get key
value
```

### DELETE命令

```shell
[127.0.0.1:7821] Moonlight> delete key
OK
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

Baili Zhang<1456938262@qq.com>