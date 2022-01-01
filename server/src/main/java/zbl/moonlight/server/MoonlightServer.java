package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.io.IoEventHandler;

import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private Configuration configuration;
    private ThreadPoolExecutor executor;
    private Engine engine;

    public static void main(String[] args) {
        MoonlightServer server = new MoonlightServer();
        server.run();
    }

    private void init() {
        configuration = new Configuration();
        configuration.setPort(7820);

        executor = new ThreadPoolExecutor(2, 4, 30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy());

        engine = new SimpleCache();
    }

    private void listen() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(configuration.getPort()));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            logger.info("moonlight server is listening at {}:{}.", "127.0.0.1", configuration.getPort());

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                CountDownLatch latch = new CountDownLatch(selectionKeys.size());

                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        executor.execute(new IoEventHandler(selectionKey, latch, selector, engine));
                        iterator.remove();
                    }
                }
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void run() {
        init();
        listen();
    }
}
