package rcache;

import rcache.common.ChannelType;
import rcache.engine.Cacheable;
import rcache.engine.simple.SimpleEngine;
import rcache.executor.Reactor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;

public class RCacheServer {
    private static final int WORKER_NUMBER = 20;

    private static final int PORT = 7820;

    public static void main(String[] args) throws IOException {
        /***
         * create cache storage engine
         */
        Cacheable cache = new SimpleEngine();

        Selector selector = Selector.open();

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(PORT));
        SelectionKey selectionKey = serverSocketChannel.register(selector,
                SelectionKey.OP_ACCEPT, ChannelType.SERVER_SOCKET_CHANNEL);

        /**
         * run reactor to accept connection
         */
        new Reactor(selector, selectionKey, cache).run();

    }
}
