package zbl.moonlight.cache.server.io;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor extends EventHandler{
    private ServerSocketChannel serverSocketChannel;
    private Selector selector;

    public Acceptor (ServerSocketChannel serverSocketChannel, Selector selector) {
        this.serverSocketChannel = serverSocketChannel;
        this.selector = selector;
    }

    @Override
    public void run() {
        try {
            SocketChannel channel = serverSocketChannel.accept();
            channel.configureBlocking(false);
            channel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(10));
            System.out.println("[ACCEPT] Accept a connection !");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
