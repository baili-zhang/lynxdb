package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {
    private static final Logger logger = LogManager.getLogger("Acceptor");
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
            channel.register(selector, SelectionKey.OP_READ, ByteBuffer.allocate(1024));
            var hostName = channel.socket().getInetAddress().getHostName();
            var port = channel.socket().getPort();
            logger.info("accept a connection, address is {}:{}.", hostName, port);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
