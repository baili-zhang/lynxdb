package zbl.moonlight.server.config;

import lombok.Getter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import zbl.moonlight.server.exception.ClusterConfigurationNotFoundException;
import zbl.moonlight.server.exception.ConfigurationException;

import java.io.*;
import java.net.URL;
import java.nio.file.Path;
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

    /**
     * socket连接数
     */
    @Getter
    private Integer connectionPoolSize;

    @Getter
    private Integer ioThreadCorePoolSize;
    @Getter
    private Integer ioThreadMaxPoolSize;

    @Getter
    private RunningMode runningMode;

    @Getter
    private ClusterConfiguration clusterConfiguration;

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
        Map<String, Object> config = loadFromConfigDirectory();
        setOption(config);
        config = load(CONFIG_FILE_NAME);
        setOption(config);
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
        String host = (String) serverOptions.get("host");
        Integer port = (Integer) serverOptions.get("port");

        if(host != null) {
            setHost(host);
        }
        if(port != null) {
            setPort(port);
        }

        /* 设置运行模式 */
        String mode = (String)config.get("mode");
        if(mode != null) {
            setRunningMode(mode);
        }

        /* 当运行模式为集群时，设置集群配置 */
        if(runningMode.equals(RunningMode.CLUSTER) && clusterConfiguration == null) {
            Map<String, Object> clusterConfig = (Map<String, Object>)config.get("cluster");
            if(clusterConfig == null) {
                throw new ClusterConfigurationNotFoundException("cluster option is not found in application.yml");
            }
            List<LinkedHashMap<String, Object>> nodes = (List<LinkedHashMap<String, Object>>) clusterConfig.get("nodes");
            setClusterConfiguration(new ClusterConfiguration(nodes));
        }
    }

    private void setDefault() {
        setHost("127.0.0.1");
        setPort(7820);
        setConnectionPoolSize(50);
        setIoThreadCorePoolSize(15);
        setIoThreadMaxPoolSize(30);
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

    private void setConnectionPoolSize(int size) {
        if(connectionPoolSize == null) {
            connectionPoolSize = size;
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

    private void setRunningMode(String mode) {
        if(runningMode == null) {
            switch (mode) {
                case "single":
                    runningMode = RunningMode.SINGLE;
                    break;
                case "cluster":
                    runningMode = RunningMode.CLUSTER;
                    break;
            }
        }
    }

    private void setClusterConfiguration(ClusterConfiguration config) {
        if(clusterConfiguration == null) {
            clusterConfiguration = config;
        }
    }
}
