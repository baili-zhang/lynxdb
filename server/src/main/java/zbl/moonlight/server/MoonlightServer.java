package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.io.HeartBeator;
import zbl.moonlight.server.cluster.ResponseOrganizer;
import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.config.RunningMode;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.exception.ConfigurationException;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.io.MdtpSocketClient;
import zbl.moonlight.server.io.MdtpSocketServer;
import zbl.moonlight.server.log.BinaryLog;
import zbl.moonlight.server.log.BinaryLogWriter;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private Configuration configuration;

    public static void main(String[] args) throws IOException,
            IncompleteBinaryLogException, ConfigurationException {
        new MoonlightServer().run();
    }

    public void run() throws IOException,
            IncompleteBinaryLogException, ConfigurationException {
        /* 读取服务器的相关配置 */
        configuration = new Configuration();
        logger.info("Read configuration completed.");

        /* 初始化事件总线 */
        EventBus eventBus = new EventBus();
        Thread eventBusThread = new Thread(eventBus, eventBus.getNAME());

        /* 初始化MDTP服务器 */
        MdtpSocketServer server = new MdtpSocketServer(configuration, eventBus);
        new Thread(server, server.getNAME()).start();
        /* 注册MDTP服务到事件总线 */
        eventBus.register(EventType.CLIENT_RESPONSE, server);
        logger.info("\"MdtpSocketServer\" thread start.");

        logger.info("Reading data from binary log file...");
        /* 初始化二进制日志文件 */
        BinaryLog binaryLog = new BinaryLog();
        /* 读取二进制日志文件，TODO:如果日志文件过大会占用大量内存，这里需要优化一下 */
        List<MdtpRequest> requests = binaryLog.read();
        /* 初始化二进制文件线程 */
        BinaryLogWriter writer = new BinaryLogWriter(binaryLog, eventBus, configuration);
        new Thread(writer, writer.getNAME()).start();
        /* 注册二进制文件线程到事件总线 */
        eventBus.register(EventType.BINARY_LOG_REQUEST, writer);
        logger.info("\"BinaryLogWriter\" thread start.");

        /* 初始化存储引擎线程 */
        SimpleCache simpleCache = new SimpleCache(configuration.getCacheCapacity(), eventBus);
        new Thread(simpleCache, simpleCache.getNAME()).start();
        /* 注册存储引擎到事件总线 */
        eventBus.register(EventType.CLIENT_REQUEST, simpleCache);
        logger.info("\"SimpleCache\" thread start.");

        /* 如果运行模式不是单节点模式，则启动心跳线程 */
        HeartBeator heartBeator = new HeartBeator();
        new Thread(heartBeator, heartBeator.getNAME()).start();

        /* 初始化集群相关模块 */
        if(RunningMode.CLUSTER.equals(configuration.getRunningMode())) {
            ClusterConfiguration clusterConfig = configuration.getClusterConfiguration();
            /* leader收到的响应数量最少应该超过count，才能算请求写入成功 */
            int count = (clusterConfig.getNodes().size() >> 1) + 1;

            /* 初始化集群客户端 */
            int i = 0;
            for(LinkedHashMap<String, Object> nodeConfig : clusterConfig.getNodes()) {
                MdtpSocketClient client = new MdtpSocketClient((String) nodeConfig.get("host"),
                        (int) nodeConfig.get("port"), eventBus);
                new Thread(client, client.getNAME() + i).start();
                /* 注册集群客户端到事件总线 */
                eventBus.register(EventType.CLUSTER_REQUEST, client);
            }

            /* 初始化集群响应组织器 */
            ResponseOrganizer organizer = new ResponseOrganizer(count, eventBus);
            new Thread(organizer, organizer.getNAME()).start();
            /* 注册集群事件响应器到事件总线 */
            eventBus.register(EventType.CLUSTER_RESPONSE, organizer);
        }

        /* 启动事件总线线程 */
        eventBusThread.start();
        /* 恢复数据 */
        for(MdtpRequest request : requests) {
            eventBus.offer(new Event<>(EventType.CLIENT_REQUEST, null, request));
        }
    }
}
