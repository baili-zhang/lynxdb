package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.cluster.RaftRpcClient;
import zbl.moonlight.server.config.RunningMode;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.mdtp.server.MdtpSocketServer;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    public static void main(String[] args) {
        new MoonlightServer().run();
    }

    public void run() {

        EventBus eventBus = MdtpServerContext.getInstance().getEventBus();
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

        /* 如果运行模式为集群，则启动RaftRpc客户端和RaftRpc服务器 */
        if(MdtpServerContext.getInstance().getConfiguration()
                .getRunningMode().equals(RunningMode.CLUSTER)) {
            Executor.start(new RaftRpcClient());
        }
    }
}
