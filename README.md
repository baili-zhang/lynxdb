# LynxDB

LynxDB is a light-weight distributed storage system implemented in the Java language and based on the Raft protocol. LynxDB supports (key, column family, column, value) structured storage.

[中文文档](docs/v1/chinese/LynxDB.md)

## Running LynxDB

The server startup script under Windows system is `start-server.bat` , and the client startup script is `start-client.bat`.

The server startup script under Linux system is `start-server.sh` , and the client startup script is `start-client.sh`.

The default port number for LynxDB server is `7820`, make sure port `7820` is not occupied by other processes.

## Spring Boot

*Maven Dependency*

```xml
<dependency>
    <groupId>com.bailizhang.lynxdb</groupId>
    <artifactId>lynxdb-spring-boot-starter</artifactId>
    <version>${lynxdb.version}</version>
</dependency>
```

*application.yml*

```yaml
com:
  bailizhang:
    lynxdb:
      host: "127.0.0.1"
      port: 7820
```

## Configuration

**Configuration file**

Dir: `/config/app.cfg`

**Configuration item**

```
host                    = 127.0.0.1
port                    = 7820
dataDir                 = [base]/data/single/base
runningMode             = single
enableFlightRecorder    = true
```

## Maintainers

See [MAINTAINERS](./MAINTAINERS)