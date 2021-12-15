package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.Method;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.engine.Cacheable;
import zbl.moonlight.server.engine.simple.SimpleCache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MoonlightServer {
    private static final Configuration config = new Configuration();
    private static final Cacheable cache = new SimpleCache();
    private static final ConcurrentLinkedQueue<Command> commandsNotExecuted = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Command> commandsExecuted = new ConcurrentLinkedQueue<>();

    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private static final int PORT = config.getPort();

    private static void doAccept(SelectionKey selectionKey, Selector selector)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
        String hostName = channel.socket().getInetAddress().getHostName();
        Integer port = channel.socket().getPort();
        logger.info("accept a connection, address is {}:{}.", hostName, port);
    }

    private static void doRead(SelectionKey selectionKey, Selector selector) throws IOException {
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        socketChannel.read(byteBuffer);
        byte code = byteBuffer.get(0);
        Command command = Command.create(code);

        int keyLength = byteBuffer.get(1) & 0xff;
        command.setKeyLength(keyLength);

        ByteBuffer key = ByteBuffer.allocateDirect(command.getKeyLength());
        socketChannel.read(key);
        command.setKey(key);

        IntBuffer valueLengthBuffer = IntBuffer.allocate(1);
        int valueLength = valueLengthBuffer.get(0);

        ByteBuffer value = ByteBuffer.allocateDirect(valueLength);
        socketChannel.read(value);
        command.setValue(value);

        logger.info("method is {}", Method.getMethodName(code));

        command.setSelectionKey(selectionKey);
        socketChannel.register(selector, SelectionKey.OP_WRITE);

        commandsNotExecuted.offer(command);
    }

    private static void accept() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(PORT));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();

                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();

                    if(selectionKey.isAcceptable()) {
                        doAccept(selectionKey, selector);
                    } else if (selectionKey.isReadable()) {
                        doRead(selectionKey, selector);
                    }
                }
                selectionKeys.clear();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void cache () {
        while (true) {
            if(commandsNotExecuted.size() == 0) {
                Thread.yield();
            } else {
                Command command = commandsNotExecuted.poll();
                cache.exec(command);
                commandsExecuted.offer(command);
            }
        }
    }

    public static void response () {
        try {
            while (true) {
                if(commandsExecuted.size() == 0) {
                    Thread.yield();
                } else {
                    Command command = commandsExecuted.poll();
                    SocketChannel socketChannel = (SocketChannel) command.getSelectionKey().channel();
                    socketChannel.write(command.getResponse());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Thread(MoonlightServer::accept, "accept").start();
        logger.info("accept thread is running.");
        new Thread(MoonlightServer::cache, "cache").start();
        logger.info("cache thread is running.");
        new Thread(MoonlightServer::response, "response").start();
        logger.info("response thread is running.");
        logger.info("moonlight server is running, listening at {}:{}.", "127.0.0.1", PORT);
    }
}
