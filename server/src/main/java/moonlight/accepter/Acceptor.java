package moonlight.accepter;

import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor implements Runnable {
    private Selector selector;
    private ServerSocketChannel serverSocketChannel;

    public Acceptor (Selector selector, ServerSocketChannel serverSocketChannel) {
        this.selector = selector;
        this.serverSocketChannel = serverSocketChannel;
    }

    @Override
    public void run() {
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            new EventHandler(socketChannel, selector);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Accept a connection !");
    }
}
