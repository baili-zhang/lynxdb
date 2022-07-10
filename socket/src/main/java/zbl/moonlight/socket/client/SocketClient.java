package zbl.moonlight.socket.client;

import lombok.Setter;
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

public class SocketClient extends Executor<SocketRequest> {
    private final static Logger logger = LogManager.getLogger("SocketClient");

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;
    private final static int DEFAULT_CORE_POOL_SIZE = 5;
    private final static int DEFAULT_MAX_POOL_SIZE = 10;

    private final Object setLock = new Object();

    private final ConcurrentHashMap<ServerNode, ConnectionContext> contexts = new ConcurrentHashMap<>();
    private final Selector selector;
    private final ThreadPoolExecutor executor;
    private final ConcurrentHashMap<ServerNode, SelectionKey> connecting = new ConcurrentHashMap<>();
    private final HashSet<SelectionKey> exitKeys = new HashSet<>();

    @Setter
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

        SelectionKey key;
        synchronized (setLock) {
            key = socketChannel.register(selector,
                    SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                    node);
        }

        connecting.put(node, key);
    }

    public void disconnect(ServerNode node) {
        if(connecting.containsKey(node)) {
            String message = String.format("Node %s is connecting, disconnect error", node);
            throw new RuntimeException(message);
        }
        if(contexts.containsKey(node)) {
            ConnectionContext context = contexts.get(node);
            SelectionKey key = context.getSelectionKey();
            /* 将主动退出的 selectionKey 加入 exitKeys，等待请求 */
            synchronized (exitKeys) {
                exitKeys.add(key);
            }
            context.addRequest(SocketRequest.newDisconnectRequest(node));
            context.lockRequestOffer();
            interrupt();
        }
    }

    public boolean isConnecting(ServerNode node) {
        return connecting.containsKey(node);
    }

    public boolean isConnected(ServerNode node) {
        return contexts.containsKey(node);
    }

    public Set<ServerNode> connectedNodes() {
        return contexts.keySet();
    }

    @Override
    protected void doAfterShutdown() {

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

            SocketRequest request = poll();
            while (request != null) {
                /* 如果是广播，则发送给所有已连接的服务器 */
                if(request.isBroadcast()) {
                    for (ServerNode node : contexts.keySet()) {
                        contexts.get(node).addRequest(request);
                    }
                    logger.info("Broadcast request to nodes: {}", contexts.keySet());
                }
                /* 如果是单播，则发送给指定的服务器 */
                else if(request.target() != null) {
                    ServerNode target = request.target();
                    ConnectionContext context = contexts.get(target);
                    if(context != null) {
                        logger.debug("Send request to node: {}", target);
                        context.addRequest(request);
                    }
                }
                else {
                    throw new RuntimeException("Can not find sending strategy for request.");
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
            ServerNode node = (ServerNode) selectionKey.attachment();

            try {
                while (!socketChannel.finishConnect()) {
                }

                /* 处理连接成功 */
                contexts.put(node, new ConnectionContext(selectionKey));
                selectionKey.interestOpsAnd(SelectionKey.OP_READ);
                /* 从连接中删除节点 */
                connecting.remove(node);

                handler.handleConnected(node);

                logger.info("Has connected to socket node {}.", node);
            } catch (ConnectException e) {
                /* 连接失败，将 node 从 connecting 集合中删除 */
                connecting.remove(node);

                try {
                    handler.handleConnectFailure(node);
                } catch (Exception exception) {
                    exception.printStackTrace();
                }

                logger.debug("Connect to socket node {} failure.", node);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void doRead() throws Exception {
            ServerNode node = (ServerNode) selectionKey.attachment();
            ConnectionContext context = contexts.get(node);

            try {
                context.read();
            } catch (SocketException e) {
                handleDisconnect();
                e.printStackTrace();
            }

            if(context.isReadCompleted()) {
                handler.handleResponse(context.socketResponse());
            }
        }

        private void doWrite() throws IOException {
            ServerNode node = (ServerNode) selectionKey.attachment();
            ConnectionContext context = contexts.get(node);
            WritableSocketRequest request = context.currentRequest();

            request.write();

            if(request.isWriteCompleted()) {
                context.removeRequest();
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
