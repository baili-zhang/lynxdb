# LynxDB

LynxDB is a light-weight distributed storage system implemented in the Java language and based on the Raft protocol. LynxDB supports (key, column family, column, value) structured storage.

## Running LynxDB

The server startup script under Windows system is `start-server.bat` , and the client startup script is `start-client.bat`.

The server startup script under Linux system is `start-server.sh` , and the client startup script is `start-client.sh`.

The default port number for LynxDB server is `7820`, and the message port number is `7263`, make sure port `7820` and `7263` is not occupied by other processes.

## Spring Boot

*Maven Dependency*

```xml
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-spring-boot-starter</artifactId>
    <version>${lynxdb.version}</version>
</dependency>
```

*Enable Annotation*

Enable annotation `@EnableLynxDb` to import LynxDB auto configuration class.

```java
@EnableLynxDb
@SpringBootApplication
public class UserServiceApplication {
	public static void main(String[] args) {
		SpringApplication.run(UserServiceApplication.class, args);
	}
}
```

*application.yml*

```yaml
com:
  bailizhang:
    lynxdb:
      host: "127.0.0.1"
      port: 7820
```

*Autowired*

```
@Autowired
private LynxDbTemplate lynxDbTemplate;
```

*Find*

```java
// lynxDbTemplate.find
byte[] find(byte[] key, byte[] columnFamily, byte[] column);
```

```java
// lynxDbTemplate.find
List<DbValue> find(byte[] key, byte[] columnFamily)
```

Find object:

```java
<T> T find(T obj, byte[]... columns)
```

Example: see insert object.

*Insert*

Insert one column:

```java
// lynxDbTemplate.insert
void insert(byte[] key, byte[] columnFamily, byte[] column, byte[] value)
```

Insert multi columns:

```java
void insert(byte[] key, byte[] columnFamily, List<DbValue> dbValues)
```

Insert object:

```java
void insert(Object obj, byte[]... columns)
```

If insert object, object must annotate by `@LynxDbColumnFamily`, `@LynxDbKey` and `@LynxDbColumn`.

For example:

```java
@LynxDbColumnFamily("article")
public class Article {
    @LynxDbKey
    private String id;

    @LynxDbColumn
    private String title;

    @LynxDbColumn
    private String body;
}
```

*Delete*

```java
// lynxDbTemplate.delete
void delete(byte[] key, byte[] columnFamily, byte[] column)
```

*Register*

```java
// lynxDbTemplate.register
void register(byte[] key, byte[] columnFamily)
```

*Deregister*

```java
// lynxDbTemplate.deregister
void deregister(byte[] key, byte[] columnFamily)
```


## Spring Cloud

*Maven Dependency*

```xml
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-spring-boot-starter</artifactId>
    <version>${lynxdb.version}</version>
</dependency>
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-spring-cloud-starter</artifactId>
    <version>${lynxdb.version}</version>
</dependency>
```

### Service Registry

*Server*

Run `start-server.bat` to start LynxDB Server.

*Client*

Enable annotation `@EnableLynxDbDiscovery` to import LynxDB registry auto configuration class and LynxDB auto configuration class, So `@EnableLynxDb` is not needed any more.

```java
@EnableLynxDbDiscovery
@SpringBootApplication
public class UserServiceApplication {
	public static void main(String[] args) {
		SpringApplication.run(UserServiceApplication.class, args);
	}
}
```

- ServiceId: `${spring.application.name}`
- InstanceId: `${spring.application.name}--[host]:${server.port}`

Service register info is stored as:

| Field         | Content           |
|---------------|-------------------|
| Key           | ServiceId         |
| Column Family | `lynxdb-registry` |
| Column        | InstanceId        |
| Value         | URL               |

## Query Command

Run `start-client.bat` to start LynxDB Cmd Client.

### Find

```shell
find [key] [column family] [column]
```

```shell
find [key] [column family] 
```

### Insert

```shell
insert [key] [column family] [column] [value]
```

### Delete

```shell
delete [key] [column family] [column]
```

### Register

```shell
register [key] [column family]
```

### Deregister

```shell
Deregister [key] [column family]
```

## Configuration

**Configuration file**

Dir: `/config/app.cfg`

**Configuration item**

```
host = 127.0.0.1
port = 7820
running_mode = single
```

## Maintainers

See [MAINTAINERS](./MAINTAINERS)