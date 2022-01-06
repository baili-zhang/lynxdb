package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.cluster.ResponseOrganizer;
import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.ClusterNodeConfiguration;
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
        configuration = new Configuration();

        /* 初始化事件总线 */
        EventBus eventBus = new EventBus();
        Thread eventBusThread = new Thread(eventBus, eventBus.getNAME());

        /* 初始化MDTP服务器 */
        MdtpSocketServer server = new MdtpSocketServer(configuration, eventBus, eventBusThread);
        Thread serverThread = new Thread(server, server.getNAME());
        /* 注册MDTP服务到事件总线 */
        eventBus.register(EventType.CLIENT_RESPONSE, server, serverThread);

        /* 初始化二进制日志文件 */
        BinaryLog binaryLog = new BinaryLog();
        /* 读取二进制日志文件 */
        List<MdtpRequest> requests = binaryLog.read();
        /* 初始化二进制文件线程 */
        BinaryLogWriter writer = new BinaryLogWriter(binaryLog, eventBus, eventBusThread);
        Thread binaryLogWriterThread = new Thread(writer, writer.getNAME());
        /* 注册二进制文件线程到事件总线 */
        eventBus.register(EventType.LOG_REQUEST, writer, binaryLogWriterThread);

        /* 初始化存储引擎线程 */
        SimpleCache simpleCache = new SimpleCache(eventBus, eventBusThread);
        Thread engine = new Thread(simpleCache, simpleCache.getNAME());
        /* 注册存储引擎到事件总线 */
        eventBus.register(EventType.CLIENT_REQUEST, simpleCache, engine);

        /* 初始化集群相关模块 */
        if(RunningMode.CLUSTER.equals(configuration.getRunningMode())) {
            ClusterConfiguration clusterConfig = configuration.getClusterConfiguration();
            /* leader收到的响应数量最少应该超过count，才能算请求写入成功 */
            int count = (clusterConfig.getNodes().size() >> 1) + 1;

            /* 初始化集群客户端 */
            int i = 0;
            for(ClusterNodeConfiguration nodeConfig : clusterConfig.getNodes()) {
                MdtpSocketClient client = new MdtpSocketClient(nodeConfig.getHost(), nodeConfig.getPort(),
                        eventBus, eventBusThread);
                Thread clientThread = new Thread(client, client.getNAME() + i);
                /* 注册集群客户端到事件总线 */
                eventBus.register(EventType.CLUSTER_REQUEST, client, clientThread);
            }

            /* 初始化集群响应组织器 */
            ResponseOrganizer organizer = new ResponseOrganizer(count, eventBus, eventBusThread);
            Thread organizerThread = new Thread(organizer, organizer.getNAME());
            /* 注册集群事件响应器到事件总线 */
            eventBus.register(EventType.CLUSTER_RESPONSE, organizer, organizerThread);
        }

        /* 启动所有注册到事件总线上的线程 */
        eventBus.start();
        /* 启动事件总线线程 */
        eventBusThread.start();
        /* 恢复数据 */
        for(MdtpRequest request : requests) {
            eventBus.offer(new Event(EventType.CLIENT_REQUEST, null, request));
        }
    }
}
