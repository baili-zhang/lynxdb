package moonlight;

import moonlight.reactor.Acceptor;
import moonlight.reactor.EventType;
import moonlight.reactor.Dispatcher;
import moonlight.reactor.WorkerPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MoonlightServer {
    private static final int WORKER_NUMBER = 20;

    private static final int PORT = 7820;

    public static void main(String[] args) throws IOException {
        /* create and init a worker pool */
        WorkerPool.init(100, 300, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(10000));

        Selector selector = Selector.open();

        /* create a dispatcher */
        Dispatcher dispatcher = new Dispatcher(selector);

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(PORT));
        SelectionKey serverSelectionKey = serverSocketChannel.register(
                selector,
                SelectionKey.OP_ACCEPT,
                new Acceptor(selector, serverSocketChannel)
        );

        System.out.println("Moonlight server is running, waiting for connect...");

        while (true) {
            dispatcher.handleEvents();
        }

    }
}
