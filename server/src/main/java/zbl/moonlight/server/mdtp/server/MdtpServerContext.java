package zbl.moonlight.server.mdtp.server;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.raft.RaftState;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.exception.ConfigurationException;

public class MdtpServerContext {
    private static final Logger logger = LogManager.getLogger("ServerContext");
    private static MdtpServerContext instance;

    static {
        try {
            instance = new MdtpServerContext();
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    @Getter
    private final EventBus eventBus;
    @Getter
    private final Configuration configuration;
    @Getter
    private final RaftState raftState;

    private MdtpServerContext() throws ConfigurationException {
        /* 读取服务器的相关配置 */
        configuration = new Configuration();
        logger.info("Read configuration completed.");

        /* 初始化事件总线 */
        eventBus = new EventBus();
        /* 初始化Raft的相关状态 */
        raftState = new RaftState();
    }

    public static MdtpServerContext getInstance() {
        return instance;
    }
}
