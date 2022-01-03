package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.subscriber.BinaryLogSubscriber;
import zbl.moonlight.server.eventbus.subscriber.ClusterSubscriber;
import zbl.moonlight.server.exception.IncompleteBinaryLogException;
import zbl.moonlight.server.io.IoEventHandler;
import zbl.moonlight.server.log.BinaryLog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private ServerContext context = ServerContext.getInstance();

    public static void main(String[] args) throws IOException, IncompleteBinaryLogException {
        MoonlightServer server = new MoonlightServer();
        server.run();
    }

    private void init() throws IOException, IncompleteBinaryLogException {
        configuration = new Configuration();

        executor = new ThreadPoolExecutor(configuration.getIoThreadCorePoolSize(),
                configuration.getIoThreadMaxPoolSize(),
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy());

        context.setEngine(new SimpleCache());

        BinaryLog binaryLog = new BinaryLog();
        binaryLog.read();

        EventBus eventBus = new EventBus();
        eventBus.register(new BinaryLogSubscriber(binaryLog));
        eventBus.register(new ClusterSubscriber());
        context.setEventBus(eventBus);
    }

    private void listen() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(configuration.getPort()));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            logger.info("moonlight server is listening at {}:{}.", configuration.getHost(), configuration.getPort());

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                CountDownLatch latch = new CountDownLatch(selectionKeys.size());

                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        executor.execute(new IoEventHandler(selectionKey, latch, selector));
                        iterator.remove();
                    }
                }
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() throws IOException, IncompleteBinaryLogException {
        init();
        listen();
    }
}
