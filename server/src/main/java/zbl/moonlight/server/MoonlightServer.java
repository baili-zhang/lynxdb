package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.exception.ConfigurationException;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.io.MdtpSocketServer;
import zbl.moonlight.server.log.BinaryLog;
import zbl.moonlight.server.log.BinaryLogWriter;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.IOException;
import java.util.List;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    public static void main(String[] args) throws IOException,
            IncompleteBinaryLogException {
        new MoonlightServer().run();
    }

    public void run() throws IOException,
            IncompleteBinaryLogException {

        EventBus eventBus = ServerContext.getInstance().getEventBus();
        Thread eventBusThread = new Thread(eventBus, eventBus.getNAME());

        /* 初始化MDTP服务器 */
        MdtpSocketServer server = new MdtpSocketServer();
        new Thread(server, server.getNAME()).start();
        /* 注册MDTP服务到事件总线 */
        eventBus.register(EventType.CLIENT_RESPONSE, server);
        logger.info("\"MdtpSocketServer\" thread start.");

        logger.info("Reading data from binary log file...");
        /* 初始化二进制日志文件 */
        BinaryLog binaryLog = new BinaryLog();
        /* 读取二进制日志文件 */
        List<MdtpRequest> requests = binaryLog.read();
        /* 初始化二进制文件线程 */
        BinaryLogWriter writer = new BinaryLogWriter(binaryLog);
        new Thread(writer, writer.getNAME()).start();
        /* 注册二进制文件线程到事件总线 */
        eventBus.register(EventType.BINARY_LOG_REQUEST, writer);
        logger.info("\"BinaryLogWriter\" thread start.");

        /* 初始化存储引擎线程 */
        SimpleCache simpleCache = new SimpleCache();
        new Thread(simpleCache, simpleCache.getNAME()).start();
        /* 注册存储引擎到事件总线 */
        eventBus.register(EventType.CLIENT_REQUEST, simpleCache);
        logger.info("\"SimpleCache\" thread start.");

        /* 启动事件总线线程 */
        eventBusThread.start();
        /* 恢复数据 */
        for(MdtpRequest request : requests) {
            eventBus.offer(new Event<>(EventType.CLIENT_REQUEST, null, request));
        }
    }
}
