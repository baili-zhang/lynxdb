package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.simple.SimpleCache;
import zbl.moonlight.server.protocol.Mdtp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MoonlightServer {
    private static final Configuration config = new Configuration();
    private static final Engine cache = new SimpleCache();
    private static final ConcurrentHashMap<SelectionKey, Mdtp> readUnfinished = new ConcurrentHashMap<>();
    private static final ConcurrentLinkedQueue<Mdtp> readFinished = new ConcurrentLinkedQueue<>();
    private static final ConcurrentLinkedQueue<Mdtp> writeUnfinished = new ConcurrentLinkedQueue<>();

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

    private static void doRead(SelectionKey selectionKey, Selector selector) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        Mdtp mdtp = readUnfinished.contains(selectionKey) ? readUnfinished.get(selectionKey) : new Mdtp(selectionKey);

        int status = mdtp.read(socketChannel);
        switch (status) {
            case Mdtp.READ_UNCOMPLETED:
                if(!readUnfinished.contains(selectionKey)) {
                    readUnfinished.put(selectionKey, mdtp);
                }
                return;
            case Mdtp.READ_COMPLETED_SOCKET_CLOSE:
                if(readUnfinished.contains(selectionKey)) {
                    readUnfinished.remove(selectionKey);
                    mdtp.setHasResponse(false);
                }
                readFinished.offer(mdtp);
                socketChannel.close();
                selectionKey.cancel();
                logger.info("close socket channel, address is {}", getAddressString(socketChannel));
                return;
            case Mdtp.READ_COMPLETED:
                if(readUnfinished.contains(selectionKey)) {
                    readUnfinished.remove(selectionKey);
                }
                readFinished.offer(mdtp);
                mdtp.setHasResponse(true);
                return;
            case Mdtp.READ_ERROR:
                if(readUnfinished.contains(selectionKey)) {
                    readUnfinished.remove(selectionKey);
                }
                socketChannel.close();
                selectionKey.cancel();
                logger.info("mdtp data read error, close socket channel, address is {}", getAddressString(socketChannel));
                return;
        }
    }

    private static void accept() {
        logger.info("accept thread is running.");

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

    public static void engine () {
        logger.info("cache engine thread is running.");

        while (true) {
            if(readFinished.size() == 0) {
                Thread.yield();
            } else {
                Mdtp mdtp = readFinished.poll();
                cache.exec(mdtp);
                writeUnfinished.offer(mdtp);
            }
        }
    }

    public static void response () {
        logger.info("response thread is running.");
        try {
            while (true) {
                if(writeUnfinished.size() == 0) {
                    Thread.yield();
                } else {
                    Mdtp mdtp = writeUnfinished.poll();
                    if (mdtp.isHasResponse()) {
                        SocketChannel socketChannel = (SocketChannel) mdtp.getSelectionKey().channel();
                        mdtp.getResponse().write(socketChannel);
                    }
                    logger.info("write data to client finished.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Thread(MoonlightServer::accept, "accept").start();
        new Thread(MoonlightServer::engine, "engine").start();
        new Thread(MoonlightServer::response, "response").start();
        logger.info("moonlight server is running, listening at {}:{}.", "127.0.0.1", PORT);
    }
}
