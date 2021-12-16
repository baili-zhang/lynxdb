package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.protocol.DecodeException;
import zbl.moonlight.server.protocol.Mdtp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MoonlightServer {
    private static final int CLIENT_CLOSE = -1;

    private static final Configuration config = new Configuration();
    private static final Engine cache = new SimpleCache();
    private static final ConcurrentHashMap<SelectionKey, Mdtp> unfinished = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<Command> commandsNotExecuted = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Command> commandsExecuted = new ConcurrentLinkedQueue<>();

    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private static final int PORT = config.getPort();

    private static String getAddressString(SocketChannel socketChannel) {
        String hostName = socketChannel.socket().getInetAddress().getHostName();
        Integer port = socketChannel.socket().getPort();
        return hostName + ":" + port;
    }

    private static void doAccept(SelectionKey selectionKey, Selector selector)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);
        channel.register(selector, SelectionKey.OP_READ);
        logger.info("accept a connection, address is {}.", getAddressString(channel));
    }

    private static void doRead(SelectionKey selectionKey, Selector selector) throws IOException, DecodeException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        Mdtp mdtp = unfinished.contains(selectionKey) ? unfinished.get(selectionKey) : new Mdtp();
        int readLength = mdtp.read(socketChannel);

        if(readLength == CLIENT_CLOSE) {
            socketChannel.close();
            selectionKey.cancel();
            logger.info("close socket channel, address is {}", getAddressString(socketChannel));
        }

        Command command = mdtp.decode();
        if(command == null) {
            if(!unfinished.contains(selectionKey)) {
                unfinished.put(selectionKey, mdtp);
            }
            return;
        }

        if(unfinished.contains(selectionKey)) {
            unfinished.remove(selectionKey);
        }


        // command.setSelectionKey(selectionKey);
        // socketChannel.register(selector, SelectionKey.OP_WRITE);
        // commandsNotExecuted.offer(command);
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
                    iterator.remove();
                }
            }
        } catch (Exception e) {
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
                    // socketChannel.write(command.getResponse());
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
