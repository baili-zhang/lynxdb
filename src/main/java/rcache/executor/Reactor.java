package rcache.executor;

import rcache.engine.Cacheable;

import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
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
                selector.select();
                selectionKeys = selector.selectedKeys();
                iterator = selectionKeys.iterator();

                ByteBuffer writeBuff = ByteBuffer.allocate(128);
                writeBuff.put("received".getBytes());
                writeBuff.flip();

                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    if(key.isAcceptable()) {
                        ServerSocketChannel serverSocketChannel =  (ServerSocketChannel) key.channel();
                        SocketChannel socketChannel = serverSocketChannel.accept();
                        socketChannel.configureBlocking(false);
                        socketChannel.register(selector,
                                SelectionKey.OP_READ, ByteBuffer.allocate(4096));

                        System.out.println("accept" );
                    }
                    if(key.isReadable()) {
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        ByteBuffer byteBuffer = (ByteBuffer) key.attachment();
                        int n = socketChannel.read(byteBuffer);
                        byteBuffer.flip();
                        String command = new String(byteBuffer.array(), 0, n).trim();
                        key.interestOps(SelectionKey.OP_WRITE);
                        System.out.println(command);
                    }
                    if(key.isWritable()) {
                        writeBuff.rewind();
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        socketChannel.write(writeBuff);
                        key.interestOps(SelectionKey.OP_READ);
                        System.out.println("write");
                    }
                    iterator.remove();
                }
            } catch (Exception exception) {
                exception.printStackTrace();
            }
        }
    }
}
