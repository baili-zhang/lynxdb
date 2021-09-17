package rcache;

import rcache.engine.Cacheable;
import rcache.engine.simple.SimpleEngine;
import rcache.executor.Reactor;
import rcache.reactor.EventType;
import rcache.reactor.InitiationDispatcher;
import rcache.reactor.SynchronousEventDemultiplexer;
import rcache.reactor.handler.AcceptEventHandler;

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

        SynchronousEventDemultiplexer demultiplexer = new SynchronousEventDemultiplexer();
        InitiationDispatcher dispatcher = new InitiationDispatcher(demultiplexer);
        dispatcher.registerHandler(new AcceptEventHandler(), EventType.ACCEPT_EVENT);

        while (true) {
            dispatcher.handleEvents();
        }

      /*  Selector selector = Selector.open();

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        ServerSocket serverSocket = serverSocketChannel.socket();
        serverSocket.bind(new InetSocketAddress(PORT));
        SelectionKey selectionKey = serverSocketChannel.register(selector,
                SelectionKey.OP_ACCEPT);*/

    }
}
