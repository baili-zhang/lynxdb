package rcache.reactor;

import rcache.reactor.handler.ReadEventHandler;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class Acceptor extends EventHandler {

    private Selector selector;

    public Acceptor (Selector selector, int port) throws IOException {
        super();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(port));
        SelectionKey selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.selectionKey = selectionKey;
        this.selector = selector;
    }

    @Override
    public void run() {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        try {
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);

            /* register socket channel and get the SelectionKey */
            SelectionKey readSelectionKey = socketChannel.register(selector, SelectionKey.OP_READ);

            /* set ByteBuffer for socket channel reading and writing */
            readSelectionKey.attach(ByteBuffer.allocate(1024));

            Dispatcher dispatcher = Dispatcher.getInstance();
            dispatcher.registerHandler(new ReadEventHandler(readSelectionKey), EventType.READ_EVENT);
            dispatcher.removeHandlingEvent(selectionKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Accept a connection !");
    }
}
