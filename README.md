# Moonlight

Moonlight 是一个高性能分布式缓存服务器，由java语言编写，采用多线程架构，实现的功能包括：

- 增删改查操作
- [MDTP通信协议](MDTP.md)
- 红黑树索引，跳表索引
- LRU缓存更新
- 数据持久化（二进制数据文件）
- 基于Raft算法的集群实现
- yaml格式配置文件

## 配置项

- 端口号
- 核心线程数
- 最大线程数

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

