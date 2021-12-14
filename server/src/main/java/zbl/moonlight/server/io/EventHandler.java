package zbl.moonlight.server.io;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public abstract class EventHandler implements Runnable {
    protected SelectionKey selectionKey;
    protected String hostName;
    protected Integer port;

    public EventHandler(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        hostName = socketChannel.socket().getInetAddress().getHostName();
        port = socketChannel.socket().getPort();
    }
}
