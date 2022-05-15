package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.server.exception.ClusterConfigurationNotFoundException;
import zbl.moonlight.server.exception.ConfigurationException;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@ToString
public class Configuration {
    private static final Logger logger = LogManager.getLogger("Configuration");
    private static final String CONFIG_FILE_NAME = "application.yml";

    @Getter
    private String host;
    @Getter
    private Integer port;
    @Getter
    private Integer backlog;

    @Getter
    /* IO线程池的核心线程数 */
    private Integer ioThreadCorePoolSize;
    @Getter
    /* IO线程池的最大线程数 */
    private Integer ioThreadMaxPoolSize;
    @Getter
    /* IO线程池的非核心线程的存活时间 */
    private Integer ioThreadKeepAliveTime;
    @Getter
    /* IO线程池的阻塞队列大小 */
    private Integer ioThreadBlockingQueueSize;

    @Getter
    /* 是否同步写二进制日志 */
    private Boolean syncWriteLog;

    @Getter
    /* cache的最大容量 */
    private Integer cacheCapacity;

    @Getter
    /* 运行模式 */
    private RunningMode runningMode;

    @Getter
    private List<ServerNode> raftNodes;

    public ServerNode currentNode() {
        return new ServerNode(host, port);
    }

    public Configuration() throws ConfigurationException {
        try {
            load();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info(this);
    }

    private Map<String, Object> loadFromConfigDirectory() throws IOException {
        String directory = System.getProperty("user.dir");
        String path = Path.of(directory, "config", CONFIG_FILE_NAME).toString();
        return new Yaml().load(new FileInputStream(path));
    }

    private void load() throws IOException, ConfigurationException {
        /* 先从user.dir目录下的配置文件获取配置信息 */
        Map<String, Object> config = loadFromConfigDirectory();
        setOption(config);
        /* 再从resources目录下的配置文件下获取配置信息 */
        config = load(CONFIG_FILE_NAME);
        setOption(config);
        /* 设置一些没有配置的配置项的默认值 */
        setDefault();
    }

    private Map<String, Object> load(String path) throws IOException {
        Yaml yaml = new Yaml();
        URL url = this.getClass().getClassLoader().getResource(path);
        if(url == null) {
            return null;
        }
        InputStream inputStream = url.openStream();

        return yaml.load(inputStream);
    }

    private void setOption (Map<String, Object> config) throws ConfigurationException {
        if(config == null) {
            return;
        }

        /* 设置server的相关配置 */
        Map<String, Object> serverOptions = (Map<String, Object>) config.get("server");
        if(serverOptions != null) {
            String host = (String) serverOptions.get("host");
            Integer port = (Integer) serverOptions.get("port");
            Integer backlog = (Integer) serverOptions.get("backlog");
            Integer ioThreadCorePoolSize = (Integer) serverOptions.get("io_thread_core_pool_size");
            Integer ioThreadMaxPoolSize = (Integer) serverOptions.get("io_thread_max_pool_size");
            Integer ioThreadKeepAliveTime = (Integer) serverOptions.get("io_thread_keep_alive_time");
            Integer ioThreadBlockingQueueSize = (Integer) serverOptions.get("io_thread_blocking_queue_size");

            if(host != null) {
                setHost(host);
            }
            if(port != null) {
                setPort(port);
            }
            if(backlog != null) {
                setBacklog(backlog);
            }
            if(ioThreadCorePoolSize != null) {
                setIoThreadCorePoolSize(ioThreadCorePoolSize);
            }
            if(ioThreadMaxPoolSize != null) {
                setIoThreadMaxPoolSize(ioThreadMaxPoolSize);
            }
            if(ioThreadKeepAliveTime != null) {
                setIoThreadKeepAliveTime(ioThreadKeepAliveTime);
            }
            if(ioThreadBlockingQueueSize != null) {
                setIoThreadBlockingQueueSize(ioThreadBlockingQueueSize);
            }
        }

        /* 设置是否同步写二进制日志 */
        Boolean sync = (Boolean) config.get("sync_write_log");
        if(sync != null) {
            setSyncWriteLog(sync);
        }

        /* 设置cache的相关配置 */
        LinkedHashMap<String, Integer> cacheConfig = (LinkedHashMap<String, Integer>) config.get("cache");
        if(cacheConfig != null) {
            Integer capacity = cacheConfig.get("capacity");
            setCacheCapacity(capacity);
        }

        /* 设置运行模式 */
        String mode = (String)config.get("mode");
        if(mode != null) {
            setRunningMode(mode);
        }

        /* 当运行模式为集群时，设置集群配置 */
        if(runningMode.equals(RunningMode.CLUSTER) && raftNodes == null) {
            Map<String, Object> clusterConfig = (Map<String, Object>)config.get("cluster");
            if(clusterConfig == null) {
                throw new ClusterConfigurationNotFoundException("cluster option is not found in application.yml");
            }
            List<LinkedHashMap<String, Object>> nodes = (List<LinkedHashMap<String, Object>>) clusterConfig.get("nodes");
            List<ServerNode> raftNodes = new ArrayList<>();
            for(LinkedHashMap<String, Object> node : nodes) {
                /* TODO:禁止魔法值（“host”,"port"） RaftNode列表应该放到Configuration中解析 */
                String host = (String) node.get("host");
                int port = (int) node.get("port");
                raftNodes.add(new ServerNode(host, port));
            }
            setRaftNodes(raftNodes);
        }
    }

    /* 设置默认配置 */
    private void setDefault() {
        setHost("127.0.0.1");
        setPort(7820);
        setBacklog(-1);
        setIoThreadCorePoolSize(30);
        setIoThreadMaxPoolSize(40);
        setIoThreadKeepAliveTime(30);
        setIoThreadBlockingQueueSize(2000);
        setCacheCapacity(2000);
        setSyncWriteLog(false);
    }

    private void setHost(String host) {
        if(this.host == null) {
            this.host = host;
        }
    }

    private void setPort(int port) {
        if(this.port == null) {
            this.port = port;
        }
    }

    private void setBacklog(int size) {
        if(backlog == null) {
            backlog = size;
        }
    }

    private void setIoThreadCorePoolSize(int size) {
        if(ioThreadCorePoolSize == null) {
            ioThreadCorePoolSize = size;
        }
    }

    private void setIoThreadMaxPoolSize(int size) {
        if(ioThreadMaxPoolSize == null) {
            ioThreadMaxPoolSize = size;
        }
    }

    private void setIoThreadKeepAliveTime(int time) {
        if(ioThreadKeepAliveTime == null) {
            ioThreadKeepAliveTime = time;
        }
    }

    private void setIoThreadBlockingQueueSize(int size) {
        if(ioThreadBlockingQueueSize == null) {
            ioThreadBlockingQueueSize = size;
        }
    }

    private void setSyncWriteLog(Boolean sync) {
        if(syncWriteLog == null) {
            syncWriteLog = sync;
        }
    }

    private void setCacheCapacity(Integer capacity) {
        if(cacheCapacity == null) {
            cacheCapacity = capacity;
        }
    }

    private void setRunningMode(String mode) {
        if(runningMode == null) {
            switch (mode) {
                case "single" -> runningMode = RunningMode.SINGLE;
                case "cluster" -> runningMode = RunningMode.CLUSTER;
            }
        }
    }

    private void setRaftNodes(List<ServerNode> nodes) {
        if(raftNodes == null) {
            raftNodes = nodes;
        }
    }
}
