package zbl.moonlight.core.socket.client;

import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.AbstractExecutor;
import zbl.moonlight.core.socket.interfaces.ResponseHandler;
import zbl.moonlight.core.socket.request.WritableSocketRequest;
import zbl.moonlight.core.socket.response.ReadableSocketResponse;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketClient extends AbstractExecutor<WritableSocketRequest> {
    private final static Logger logger = LogManager.getLogger("SocketClient");

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;
    private final static int DEFAULT_CORE_POOL_SIZE = 5;
    private final static int DEFAULT_MAX_POOL_SIZE = 10;

    private final ConcurrentHashMap<ServerNode, ConnectionContext> contexts = new ConcurrentHashMap<>();
    private final Selector selector;
    private final ThreadPoolExecutor executor;

    private boolean closed = false;
    @Setter
    private ResponseHandler handler;

    public SocketClient() throws IOException {
        selector = Selector.open();

        /* 线程池执行器 */
        executor = new ThreadPoolExecutor(DEFAULT_CORE_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_CAPACITY),
                new ClientThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public void connect(ServerNode node) throws IOException {
        SocketAddress address = new InetSocketAddress(node.host(), node.port());
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(address);

        synchronized (selector) {
            socketChannel.register(selector, SelectionKey.OP_CONNECT, node);
        }
    }

    public void close() {
        closed = true;
    }

    @Override
    public void run() {
        if(handler == null) {
            throw new RuntimeException("Handler can not be null.");
        }

        try {
            /* TODO:实现优雅关机 */
            while (!closed) {
                selector.select();
                Set<SelectionKey> keys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = keys.iterator();
                CountDownLatch latch = new CountDownLatch(keys.size());

                /* 对Set进行迭代，不同步处理的话，可能会出问题 */
                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey key = iterator.next();
                        try {
                            executor.execute(new IoEventHandler(latch, key));
                        } catch (RejectedExecutionException e) {
                            latch.countDown();
                            e.printStackTrace();
                        }
                        iterator.remove();
                    }
                }

                latch.await();

                WritableSocketRequest request = poll();
                while (request != null) {
                    /* 如果是广播，则发送给所有已连接的服务器 */
                    if(request.isBroadcast()) {
                        for (ServerNode node : contexts.keySet()) {
                            contexts.get(node).offerRequest(request);
                        }
                    }
                    /* 如果是单播，则发送给指定的服务器 */
                    else if(request.getServerNode() != null) {
                        contexts.get(request.getServerNode()).offerRequest(request);
                    }
                    else {
                        throw new RuntimeException("Can not find sending strategy for request.");
                    }
                    request = poll();
                }
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /* IO线程的线程工厂 */
    private static class ClientThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ClientThreadFactory() {
            namePrefix = "Socket-Client-IO-";
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, namePrefix + threadNumber.getAndIncrement());
        }
    }

    /* 处理IO事件的线程 */
    private class IoEventHandler implements Runnable {
        private final CountDownLatch latch;
        private final SelectionKey selectionKey;

        public IoEventHandler(CountDownLatch latch, SelectionKey selectionKey) {
            this.latch = latch;
            this.selectionKey = selectionKey;
        }

        @Override
        public void run() {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            try {
                if(!socketChannel.isConnected()) {
                    if (selectionKey.isConnectable()) {
                        doConnect();
                    }
                } else {
                    if (selectionKey.isReadable()) {
                        doRead();
                    } else if (selectionKey.isWritable()) {
                        doWrite();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                latch.countDown();
            }
        }

        private void doConnect() throws IOException {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
            ServerNode node = (ServerNode) selectionKey.attachment();

            try {
                while (!socketChannel.finishConnect()) {
                }
                logger.info("Has connected to raft node {}.", node);
            } catch (ConnectException e) {
                logger.info("Connect to raft node {} failure.", node);
                return;
            }
            contexts.put(node, new ConnectionContext(selectionKey));
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }

        private void doRead() throws IOException {
            ServerNode node = (ServerNode) selectionKey.attachment();
            ConnectionContext context = contexts.get(node);
            ReadableSocketResponse response = context.getResponse();

            try {
                response.read();
            } catch (SocketException e) {
                handleDisconnect();
                e.printStackTrace();
            }

            if(response.isReadCompleted()) {
                handler.handle(response);
                context.replaceResponse();
            }
        }

        private void doWrite() throws IOException {
            ServerNode node = (ServerNode) selectionKey.attachment();
            ConnectionContext context = contexts.get(node);
            WritableSocketRequest request = context.peekRequest();

            assert request != null;
            request.write();

            if(request.isWriteCompleted()) {
                context.pollRequest();
            }
        }

        /**
         * 处理节点断开连接的情况
         */
        private void handleDisconnect() {
            ServerNode node = (ServerNode) selectionKey.attachment();
            contexts.remove(node);
            selectionKey.cancel();
            logger.info("Raft Node {} is disconnect.", node);
        }
    }
}
