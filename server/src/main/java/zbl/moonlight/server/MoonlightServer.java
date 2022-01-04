package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.ClusterNodeConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.subscriber.BinaryLogSubscriber;
import zbl.moonlight.server.eventbus.subscriber.ClusterSubscriber;
import zbl.moonlight.server.exception.ConfigurationException;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.io.MdtpSocketClient;
import zbl.moonlight.server.io.MdtpSocketServer;
import zbl.moonlight.server.log.BinaryLog;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private ServerContext context = ServerContext.getInstance();

    public static void main(String[] args) throws IOException, IncompleteBinaryLogException, ConfigurationException {
        MoonlightServer server = new MoonlightServer();
        server.run();
    }

    public void run() throws IOException, IncompleteBinaryLogException, ConfigurationException {
        init();
        MdtpSocketServer server = new MdtpSocketServer(configuration.getPort(), executor,
                new ConcurrentLinkedQueue<>(),
                new ConcurrentHashMap<>());
        server.listen();
    }

    private void init() throws IOException, IncompleteBinaryLogException, ConfigurationException {
        configuration = new Configuration();

        executor = new ThreadPoolExecutor(configuration.getIoThreadCorePoolSize(),
                configuration.getIoThreadMaxPoolSize(),
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy());

        context.setEngine(new SimpleCache());

        EventBus eventBus = new EventBus();

        BinaryLog binaryLog = new BinaryLog();
        binaryLog.read();
        eventBus.register(new BinaryLogSubscriber(binaryLog));

        /* 注册集群事件的订阅者 */
        eventBus.register(new ClusterSubscriber(configuration.getClusterConfiguration()));
        context.setEventBus(eventBus);
    }
}
