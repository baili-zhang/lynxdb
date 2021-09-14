package rcache.executor;

import rcache.common.ChannelType;
import rcache.engine.Cacheable;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class Reactor implements Runnable{
    private Selector selector;
    private SelectionKey selectionKey;
    private Cacheable cache;

    public Reactor (Selector selector, SelectionKey selectionKey, Cacheable cache) {
        this.selector = selector;
        this.selectionKey = selectionKey;
        this.cache = cache;
    }

    @Override
    public void run() {
        Set<SelectionKey> selectionKeys;
        Iterator<SelectionKey> iterator;

        System.out.println("RCache is running, waiting for connect...");
        while (true) {
            try {
                int n = selector.select();
                if(n > 1) System.out.println(n);
                selectionKeys = selector.selectedKeys();
                iterator = selectionKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if(key.isAcceptable()) {
                        if(key.attachment() == ChannelType.SERVER_SOCKET_CHANNEL) {
                            ServerSocketChannel serverSocketChannel =  (ServerSocketChannel) key.channel();
                            SocketChannel socketChannel = serverSocketChannel.accept();
                            socketChannel.configureBlocking(false);
                            socketChannel.register(selector,
                                    SelectionKey.OP_READ | SelectionKey.OP_WRITE | SelectionKey.OP_CONNECT,
                                    ChannelType.SOCKET_CHANNEL);
                        }
                    }

                    if(key.isReadable()) {
                        System.out.println("isReadable");
                    }
                }

                selectionKeys.clear();
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}
