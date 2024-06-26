# LynxDB 数据库（Version 1.0）

LynxDB 支持（列族，列，键，值）的表结构存储，存储引擎基于 LSM Tree 实现。目前，LynxDB 支持插入，查询，删除和范围查找等数据操作，不支持事务。LynxDB 使用 Java 语言编写，目前只有 Java 客户端，其他语言的客户端将会在以后补充。

LynxDB 在 Version 1.0 版本不支持分布式集群，基于 Raft 的高可用集群将会在以后的版本中实现。LynxDB 1.0 的主要目标是实现一个稳定以及高性能的单机数据库，并为以后的数据库集群提供高性能节点。

> **LynxDB 的版本规则**
> 
> 开发版本以发布的时间作为版本号，例如 `2023.10.5-snapshot`，正式的版本则会以 `1.0.0` 作为版本号。主要是为了能够很清晰的了解开发版本的发布时间。

## LynxDB 的功能简介

LynxDB 支持插入（`insert`），查询 Key 的值（`find`），删除（`delete`），查询 Key 在主列上是否存在（`exist`），向后的范围查找（`range-next`），向前的范围查找（`range-before`）。

## 项目启动

LynxDB 项目由 Java 语言编写，可以直接用 `java -jar` 的方式启动。其中 `lynxdb-server-*.jar` 是 LynxDB 服务器的包，`lynxdb-cmd-*.jar` 是 LynxDB 客户端的包。

**启动服务器**

```shell
java -Xmx1g -Xms1g -XX:+UseZGC -jar lib/lynxdb-server-2024.5.3-alpha.jar
```

**启动客户端**

```shell
java -jar lib/lynxdb-cmd-2024.5.3-alpha.jar
```

## Systemd 配置

将 lynxdb.service 文件复制到 `/etc/systemd/system/`，然后使用 `systemctl daemon-reload` 命令重新加载配置，使用 `systemctl start lynxdb` 变可以启动 LynxDB 服务。

start-server.sh 文件如下：

```shell
#!/bin/bash
java -Dlynxdb.baseDir=/root/lynxdb-v2024.5.3-alpha/\
     -Xmx256m -Xms256m\
     -XX:+UseZGC\
     -jar /root/lynxdb-v2024.5.3-alpha/lib/lynxdb-server-2024.5.3-alpha.jar
```

lynxdb.service 配置文件如下：

```text
[Unit]
Description=LynxDB Server
After=network.target

[Service]
ExecStart=/root/lynxdb-v2024.5.3-alpha/start-server.sh
Restart=on-failure
Type=simple

[Install]
WantedBy=multi-user.target
Alias=lynxdb.service
```

## 配置文件

默认配置文件：`[lynxdb-path]/config/app.cfg`

```text
host                    = 127.0.0.1
port                    = 7820
dataDir                 = [base]/data/single/base
runningMode             = single
enableFlightRecorder    = true
```

目前 LynxDB 数据库配置的选项比较少，存储引擎的相关参数将会在以后的版本添加。

配置项说明：

| 配置项                  | 描述        |
|----------------------|-----------|
| host                 | ip 或域名    |
| port                 | 端口号       |
| dataDir              | 数据目录      |
| runningMode          | 运行模式      |
| enableFlightRecorder | 是否启动飞行记录器 |

## Java 语言客户端

**Maven 依赖**

```xml
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-client</artifactId>
    <version>2024.5.3-alpha</version>
</dependency>
```

**使用案例**

简单的插入和查询数据的。

```java
public class LynxDbClientDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            byte[] value = G.I.toBytes("value");
            connection.insert(key, "columnFamily", "column", value);

            byte[] findValue = connection.find(key, "columnFamily", "column");
            System.out.println(G.I.toString(findValue));

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Insert 插入数据**

```java
public class InsertKeyDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            byte[] value = G.I.toBytes("value");
            connection.insert(key, "columnFamily", "column", value);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Insert 插入多列数据**

```java
public class InsertMultiColumnsDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);

            byte[] key = G.I.toBytes("key");
            HashMap<String, byte[]> multiColumns = new HashMap<>();

            for(int i = 0; i < 10; i ++) {
                String column = "column" + i;
                byte[] value = G.I.toBytes("value" + i);

                multiColumns.put(column, value);
            }

            connection.insert(key, "columnFamily", multiColumns);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Insert 插入 Java 对象**

`@LynxDbColumnFamily("insert-object")` 表示数据插入的 Column Family 为 `"insert-object"`，`@LynxDbKey` 用来标记 Key 字段，`@LynxDbColumn` 用来标记 Column 字段，Java Object 与数据存储中的对应关系为：

Java 对象：

`{key="key", column0="value0", column1="value1", column2="value2"}`

数据表中存储：

| key   | column0  | column1  | column2  |
|-------|----------|----------|----------|
| "key" | "value0" | "value1" | "value2" |

```java
public class InsertObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);

            DemoObject demoObject = new DemoObject();
            demoObject.setKey("key");
            demoObject.setColumn0("value0");
            demoObject.setColumn1("value1");
            demoObject.setColumn2("value2");

            connection.insert(demoObject);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    private static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Insert 插入超时数据**

```java
public class InsertTimeoutKeyDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            byte[] value = G.I.toBytes("value");
            long timeout = System.currentTimeMillis() + 30 * 1000; // 数据在 30s 后超时
            connection.insert(key, "columnFamily", "column", timeout, value);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Insert 插入多列超时数据**

给多个列同时设置同一个超时时间。

```java
public class InsertMultiTimeoutColumnsDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);

            byte[] key = G.I.toBytes("key");
            HashMap<String, byte[]> multiColumns = new HashMap<>();

            for(int i = 0; i < 10; i ++) {
                String column = "column" + i;
                byte[] value = G.I.toBytes("value" + i);

                multiColumns.put(column, value);
            }

            long timeout = System.currentTimeMillis() + 30 * 1000; // 数据在 30s 后超时

            connection.insert(key, "columnFamily", timeout, multiColumns);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Insert 插入超时 Java 对象**

```java
public class InsertTimeoutObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);

            DemoObject demoObject = new DemoObject();
            demoObject.setKey("key");
            demoObject.setColumn0("value0");
            demoObject.setColumn1("value1");
            demoObject.setColumn2("value2");

            long timeout = System.currentTimeMillis() + 30 * 1000;

            connection.insert(demoObject, timeout);

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    private static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Find 查询数据**

```java
public class FindByKeyDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            byte[] value = connection.find(key, "columnFamily", "column");

            System.out.println(G.I.toString(value));
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Find 查询多个列**

```java
public class FindMultiColumnsDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            HashMap<String, byte[]> multiColumns  = connection.findMultiColumns(key, "columnFamily");

            multiColumns.forEach((column, value) -> System.out.println(column + ": " + G.I.toString(value)));
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Find 查找 Java 对象**

```java
public class FindByClassDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            DemoObject demoObject = connection.find("key", DemoObject.class);

            System.out.println(demoObject);
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    public static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Exist 查询 Key 是否存在**

```java
public class ExistKeyDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            boolean isExisted = connection.existKey(key, "columnFamily", "column");

            System.out.println(isExisted);
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Exist 查询 Key 是否存在（Java Object 方式）**

`@LynxDbMainColumn` 用来标记主列，查询的是被标记的列上 Key 是否存在。

```java
public class ExistKeyByObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);

            DemoObject demoObject = new DemoObject();
            demoObject.setKey("key");
            boolean isExisted = connection.existKey(demoObject);

            System.out.println(isExisted);
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    public static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbMainColumn
        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Range Next 向后的范围查找**

```java
public class RangeNextDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] beginKey = G.I.toBytes("");
            List<Pair<byte[], HashMap<String, byte[]>>> multiColumns = connection.rangeNext(
                    "columnFamily",
                    "column",
                    beginKey,
                    10
            );

            multiColumns.forEach(pair -> {
                System.out.println("Key: " + G.I.toString(pair.left()));
                pair.right().forEach((column, value) ->
                        System.out.println("Column: " + column + ", " + "Value: " + G.I.toString(value)));
            });
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Range Next 向后的范围查找（Java Object 的方式返回）**

```java
public class RangeNextObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] beginKey = G.I.toBytes("");
            List<DemoObject> demoObjects = connection.rangeNext(DemoObject.class, beginKey, 10);

            System.out.println(demoObjects);
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    public static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbMainColumn
        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Range Before 向前的范围查找**

```java
public class RangeBeforeDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] beginKey = G.I.toBytes("key9");
            List<Pair<byte[], HashMap<String, byte[]>>> multiColumns = connection.rangeBefore(
                    "columnFamily",
                    "column",
                    beginKey,
                    10
            );

            multiColumns.forEach(pair -> {
                System.out.println("Key: " + G.I.toString(pair.left()));
                pair.right().forEach((column, value) ->
                        System.out.println("Column: " + column + ", " + "Value: " + G.I.toString(value)));
            });
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Range Before 向前的范围查找（Java Object 的方式返回）**

```java
public class RangeBeforeObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] beginKey = G.I.toBytes("key9");
            List<DemoObject> demoObjects = connection.rangeBefore(DemoObject.class, beginKey, 10);

            System.out.println(demoObjects);
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    public static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbMainColumn
        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

**Delete 删除 Key**

```java
public class DeleteKeyDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            connection.delete(key, "columnFamily", "column");

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Delete 删除多个 Column 的 Key**

*`deleteMultiColumns()` 方法设计的冗余了，未来将会删掉。*

```java
public class DeleteMultiColumnsDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            byte[] key = G.I.toBytes("key");
            connection.deleteMultiColumns(key, "columnFamily", "column", "column1");

        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }
}
```

**Delete 删除 Key（通过 Java Object）**

```java
public class DeleteByObjectDemo {
    public static void main(String[] args) {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        try(LynxDbClient client = new LynxDbClient()) {
            client.start();

            LynxDbConnection connection = client.createConnection("127.0.0.1", 7820);
            
            DemoObject demoObject = new DemoObject();
            demoObject.setKey("key");
            
            connection.delete(demoObject);
            
        } catch (ConnectException e) {
            e.getStackTrace();
        }
    }

    @Data
    @LynxDbColumnFamily("demo-object")
    public static class DemoObject {
        @LynxDbKey
        private String key;

        @LynxDbColumn
        private String column0;

        @LynxDbColumn
        private String column1;

        @LynxDbColumn
        private String column2;
    }
}
```

## Spring Boot

**Maven 依赖**

```xml
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-spring-boot-starter</artifactId>
    <version>2024.5.3-alpha</version>
</dependency>
```

**包扫描配置**

`com.bailizhang.lynxdb.springboot.starter` 是 lynxdb-spring-boot-starter 需要配置扫描的包，`com.bailizhang.website` 是当前应用需要扫描的包。

```java
@ComponentScan({ "com.bailizhang.lynxdb.springboot.starter", "com.bailizhang.website" })
@SpringBootApplication
public class WebsiteApplication {

    public static void main(String[] args) {
        SpringApplication.run(WebsiteApplication.class, args);
    }

}
```

**application.yml**

```yaml
com:
  bailizhang:
    lynxdb:
      host: "127.0.0.1"
      port: 7820
```

## LynxDB 命令行

**Insert 命令**

插入数据：

```shell
insert [key] [columnFamily] [column] [value]
```

**Find 命令**

查询单个 column：

```shell
find [key] [columnFamily] [column]
```

查询多个 column：

```shell
find [key] [columnFamily]
```

**Delete 命令**

删除数据：

```shell
delete [key] [columnFamily] [column]
```

**Exist 命令**

查询 Key 是否存在：

```shell
exist [key] [columnFamily] [column]
```

**Range Next 命令**

从 beginKey 往后范围查找：

```shell
range-next [columnFamily] [mainColumn] [beginKey] [limit]
```

**Range Before 命令**

从 endKey 往前范围查找：

```shell
range-before [columnFamily] [mainColumn] [endKey] [limit]
```

## LynxDB 源码相关

LynxDB 的各个模块：

- lynxdb-client：LynxDB Java 客户端相关。
- lynxdb-cmd：LynxDB 命令行客户端。
- lynxdb-core：公共类和工具类模块。
- lynxdb-ldtp：通讯协议相关的常量。
- lynxdb-lsmtree：基于 LSM Tree 的存储引擎。
- lynxdb-raft：Raft 协议相关。
- lynxdb-server：LynxDB 服务器。
- lynxdb-socket：基于 Java NIO 相关的 Socket 封装。
- lynxdb-spring-boot-starter：LynxDB 的 SpringBoot Starter。
- lynxdb-timewheel：单线程的时间轮。

## LynxDB 文件格式

[相关文档](FileFormat.md)