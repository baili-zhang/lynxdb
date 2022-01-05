package zbl.moonlight.server.io;

import lombok.Getter;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.executor.Executor;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * 实现MDTP协议的客户端
 */
public class MdtpSocketClient extends Executor<Event<?>> {
    @Getter
    private final String NAME = "MdtpSocketClient";
    private final String host;
    private final int port;

    public MdtpSocketClient(String host, int port, EventBus eventBus, Thread eventBusThread) {
        super(eventBus, eventBusThread);
        this.host = host;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(host, port));
            socketChannel.configureBlocking(false);
            Selector selector = Selector.open();
            socketChannel.register(selector, SelectionKey.OP_CONNECT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
