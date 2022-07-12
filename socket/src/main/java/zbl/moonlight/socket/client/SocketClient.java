package zbl.moonlight.socket.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.request.SocketRequest;
import zbl.moonlight.socket.request.WritableSocketRequest;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketClient extends Executor<WritableSocketRequest> {
    private final static Logger logger = LogManager.getLogger("SocketClient");

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;
    private final static int DEFAULT_CORE_POOL_SIZE = 5;
    private final static int DEFAULT_MAX_POOL_SIZE = 10;

    private final Object setLock = new Object();

    private final ConcurrentHashMap<SelectionKey, ConnectionContext> contexts
            = new ConcurrentHashMap<>();
    private final Selector selector;
    private final ThreadPoolExecutor executor;
    private final HashSet<SelectionKey> exitKeys = new HashSet<>();

    private SocketClientHandler handler;

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

        synchronized (setLock) {
            socketChannel.register(selector,
                    SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }

    }

    public void disconnect(ServerNode node) {
        if(contexts.containsKey(node)) {
            ConnectionContext context = contexts.get(node);
            SelectionKey key = context.selectionKey();
            /* 将主动退出的 selectionKey 加入 exitKeys，等待请求 */
            synchronized (exitKeys) {
                exitKeys.add(key);
            }
            // TODO: 忘了这里需要处理什么了
            context.lockRequestOffer();
            interrupt();
        }
    }

    public void setHandler(SocketClientHandler handler) {
        this.handler = handler;
    }

    @Override
    public void execute() {
        try {
            selector.select();
            /* 如果线程被中断，则将线程中断位复位 */
            if(Thread.interrupted()) {
                logger.debug("Socket client has bean interrupted.");
            }

            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
            CountDownSync sync = new CountDownSync(keys.size());

            /* 对Set进行迭代，不同步处理的话，可能会出问题 */
            synchronized (setLock) {
                while (iterator.hasNext()) {
                    SelectionKey key = iterator.next();
                    try {
                        executor.execute(new IoEventHandler(sync, key));
                    } catch (RejectedExecutionException e) {
                        sync.countDown();
                        e.printStackTrace();
                    }
                    iterator.remove();
                }
            }

            sync.await();

            /* sync.await() 后执行一些自定义的操作 */
            handler.handleAfterLatchAwait();

            WritableSocketRequest request = poll();
            while (request != null) {
                /* 如果是广播，则发送给所有已连接的服务器 */
                if(request.isBroadcast()) {
                    for (SelectionKey selectionKey : contexts.keySet()) {
                        ConnectionContext context = contexts.get(selectionKey);
                        context.offerRequest(request);
                    }
                    logger.info("Broadcast request to nodes: {}", contexts.keySet());
                }
                /* 如果是单播，则发送给指定的服务器 */
                else {
                    ConnectionContext context = contexts.get(request.selectionKey());
                    context.offerRequest(request);
                }
                request = poll();
            }
        } catch (Exception e) {
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
        private final CountDownSync sync;
        private final SelectionKey selectionKey;

        public IoEventHandler(CountDownSync sync, SelectionKey selectionKey) {
            this.sync = sync;
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
                sync.countDown();
            }
        }

        private void doConnect() {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

            try {
                while (!socketChannel.finishConnect()) {
                }

                /* 处理连接成功 */
                contexts.put(selectionKey, new ConnectionContext(selectionKey));
                selectionKey.interestOpsAnd(SelectionKey.OP_READ);

                handler.handleConnected(selectionKey);

                logger.info("Has connected to socket node {}.", socketChannel.getRemoteAddress());
            } catch (ConnectException e) {
                try {
                    handler.handleConnectFailure(selectionKey);
                    logger.debug("Connect to socket node {} failure.", socketChannel.getRemoteAddress());
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void doRead() throws Exception {
            ConnectionContext context = contexts.get(selectionKey);

            try {
                context.read();
            } catch (SocketException e) {
                handleDisconnect();
                e.printStackTrace();
            }

            if(context.isReadCompleted()) {
                handler.handleResponse(context.fetchResponse());
            }
        }

        private void doWrite() throws IOException {
            ConnectionContext context = contexts.get(selectionKey);
            WritableSocketRequest request = context.peekRequest();

            request.write();

            if(request.isWriteCompleted()) {
                context.pollRequest();
                /* 处理主动退出的 selectionKey */
                if(exitKeys.contains(selectionKey) && context.sizeOfRequests() == 0) {
                    synchronized (exitKeys) {
                        exitKeys.remove(selectionKey);
                    }
                    handleDisconnect();
                }
            }
        }

        /**
         * 处理节点断开连接的情况
         */
        private void handleDisconnect() {
            ServerNode node = (ServerNode) selectionKey.attachment();
            contexts.remove(node);
            selectionKey.cancel();
            logger.info("Disconnect from node [{}].", node);
        }
    }
}
