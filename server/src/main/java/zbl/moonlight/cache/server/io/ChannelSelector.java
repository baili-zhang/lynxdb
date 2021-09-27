package zbl.moonlight.cache.server.io;

import zbl.moonlight.cache.server.io.handler.ReadEventHandler;
import zbl.moonlight.cache.server.io.handler.WriteEventHandler;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelSelector extends EventHandler {
    private ServerSocketChannel serverSocketChannel;
    private Selector selector;

    public ChannelSelector(int port) {
        try {
            this.selector = Selector.open();
            this.serverSocketChannel = ServerSocketChannel.open();
            this.serverSocketChannel.configureBlocking(false);
            this.serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.register(
                    this.selector,
                    SelectionKey.OP_ACCEPT
            );

            System.out.println("Moonlight server is running, waiting for connect...");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void doAccept () {
        new Acceptor(serverSocketChannel, selector).run();
    }

    private void doRead (SelectionKey selectionKey) {
        new ReadEventHandler(selectionKey).run();
    }

    private void doWrite (SelectionKey selectionKey) {
        new WriteEventHandler(selectionKey).run();
    }

    @Override
    public void run() {
        try {
            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();

                    if(selectionKey.isAcceptable()) {
                        doAccept();
                    } else if (selectionKey.isReadable()) {
                        doRead(selectionKey);
                    } else if (selectionKey.isWritable()) {
                        doWrite(selectionKey);
                    }
                }
                selectionKeys.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
