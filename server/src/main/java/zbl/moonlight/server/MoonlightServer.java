package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.server.raft.RaftRpcClient;
import zbl.moonlight.server.config.RunningMode;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.mdtp.server.MdtpSocketServer;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }

    public void run() throws IOException {

        MdtpServerContext context = MdtpServerContext.getInstance();
        EventBus eventBus = context.getEventBus();
        Thread eventBusThread = new Thread(eventBus, eventBus.getClass().getSimpleName());

        /* 注册MDTP服务到事件总线 */
        eventBus.register(EventType.CLIENT_RESPONSE, Executor.start(new MdtpSocketServer()));

        /* 注册存储引擎到事件总线 */
        Executable simpleCache = Executor.start(new SimpleCache());
        eventBus.register(EventType.CLIENT_REQUEST, simpleCache);

        /* 启动事件总线线程 */
        eventBusThread.start();

        /* 如果运行模式为集群，则启动RaftRpc客户端和RaftRpc服务器 */
        if(MdtpServerContext.getInstance().getConfiguration()
                .getRunningMode().equals(RunningMode.CLUSTER)) {
            eventBus.register(EventType.CLUSTER_RESPONSE, simpleCache);
            eventBus.register(EventType.CLUSTER_REQUEST, Executor.start(new RaftRpcClient()));
        }

        /* 从日志文件中恢复数据 */
        // context.getRaftState().getRaftLog().recover();
    }
}
