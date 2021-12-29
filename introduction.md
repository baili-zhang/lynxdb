# MoonlightServer

## 启动实例

```
MoonlightServer server = new MoonlightServer();
server.run();
```

## 实例的启动流程

### init()

- 读取配置文件
- 初始化事件处理线程池

### listen()

- 监听端口，等待连接