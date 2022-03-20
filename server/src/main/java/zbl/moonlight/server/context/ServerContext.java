package zbl.moonlight.server.context;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.exception.ConfigurationException;

public class ServerContext {
    private static final Logger logger = LogManager.getLogger("ServerContext");
    private static ServerContext instance;

    static {
        try {
            instance = new ServerContext();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    @Getter
    private final EventBus eventBus;
    @Getter
    private final Configuration configuration;

    private ServerContext() throws ConfigurationException {
        /* 读取服务器的相关配置 */
        configuration = new Configuration();
        logger.info("Read configuration completed.");

        /* 初始化事件总线 */
        eventBus = new EventBus();
    }

    public static ServerContext getInstance() {
        return instance;
    }
}
