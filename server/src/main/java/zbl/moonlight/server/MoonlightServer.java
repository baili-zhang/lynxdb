package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.cluster.HeartBeator;
import zbl.moonlight.server.cluster.RaftRpcClient;
import zbl.moonlight.server.cluster.RaftRpcServer;
import zbl.moonlight.server.config.RunningMode;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.io.MdtpSocketServer;
import zbl.moonlight.server.log.BinaryLog;
import zbl.moonlight.server.log.BinaryLogWriter;

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
        Thread eventBusThread = new Thread(eventBus, eventBus.getClass().getSimpleName());

        /* 注册MDTP服务到事件总线 */
        eventBus.register(EventType.CLIENT_RESPONSE, Executor.start(new MdtpSocketServer()));

        // logger.info("Reading data from binary log file...");
        /* 初始化二进制日志文件 */
        // BinaryLog binaryLog = new BinaryLog();
        /* 读取二进制日志文件 */
        // List<MdtpRequest> requests = binaryLog.read();
        /* 注册二进制文件线程到事件总线 */
        // eventBus.register(EventType.BINARY_LOG_REQUEST, Executor.start(new BinaryLogWriter(binaryLog)));

        /* 注册存储引擎到事件总线 */
        eventBus.register(EventType.ENGINE_REQUEST, Executor.start(new SimpleCache()));

        /* 启动事件总线线程 */
        eventBusThread.start();
        /* 恢复数据 */
        // for(MdtpRequest request : requests) {
        //     eventBus.offer(new Event(EventType.CLIENT_REQUEST, request));
        // }

        /* 如果运行模式为集群，则启动心跳线程和RaftRpc服务器 */
        if(ServerContext.getInstance().getConfiguration()
                .getRunningMode().equals(RunningMode.CLUSTER)) {
            RaftRpcClient client = new RaftRpcClient();
            Executor.start(client);
            Executor.start(new HeartBeator(client));
            Executor.start(new RaftRpcServer());
        }
    }
}
