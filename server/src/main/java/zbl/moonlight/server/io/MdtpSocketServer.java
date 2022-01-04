package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 实现MDTP协议的服务器
 */
public class MdtpSocketServer {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");

    private final int port;
    private final ThreadPoolExecutor executor;
    private final ConcurrentLinkedQueue<MdtpRequest> requests;
    ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap;

    public MdtpSocketServer(int port, ThreadPoolExecutor executor, ConcurrentLinkedQueue<MdtpRequest> requests,
                            ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap) {
        this.port = port;
        this.executor = executor;
        this.requests = requests;
        this.responsesMap = responsesMap;
    }

    public void listen() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(port));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                CountDownLatch latch = new CountDownLatch(selectionKeys.size());

                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        executor.execute(new IoEventHandler(selectionKey, latch, selector, requests, responsesMap));
                        iterator.remove();
                    }
                }
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
