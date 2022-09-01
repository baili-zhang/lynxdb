package com.bailizhang.lynxdb.socket.client;

import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.request.WritableSocketRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.executor.Executor;

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
    private final AtomicInteger serial = new AtomicInteger(0);

    private final ConcurrentHashMap<SelectionKey, ConnectionContext> contexts
            = new ConcurrentHashMap<>();
    private final Selector selector;
    private final ThreadPoolExecutor executor;
    private final HashSet<SelectionKey> exitKeys = new HashSet<>();

    private SocketClientHandler handler;

    public SocketClient() {
        try {
            selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        /* 线程池执行器 */
        executor = new ThreadPoolExecutor(DEFAULT_CORE_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_CAPACITY),
                new ClientThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    public SelectionKey connect(ServerNode node) throws IOException {
        SocketAddress address = new InetSocketAddress(node.host(), node.port());
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(address);

        SelectionKey selectionKey;

        synchronized (setLock) {
            selectionKey = socketChannel.register(selector,
                    SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        }

        contexts.put(selectionKey, new ConnectionContext(selectionKey));

        interrupt();
        return selectionKey;
    }

    public void disconnect(SelectionKey selectionKey) {
        if(contexts.containsKey(selectionKey)) {
            ConnectionContext context = contexts.get(selectionKey);
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

    @Override
    public void offerInterruptibly(WritableSocketRequest request) {
        ConnectionContext context = contexts.get(request.selectionKey());
        context.offerRequest(request);
        interrupt();
    }

    public boolean isConnected(SelectionKey selectionKey) {
        return contexts.containsKey(selectionKey);
    }

    public final int send(SelectionKey selectionKey, byte[] data) {
        byte status = (byte) 0x00;
        int requestSerial = serial.getAndIncrement();

        WritableSocketRequest request = new WritableSocketRequest(
                selectionKey,
                status,
                requestSerial,
                data
        );

        offerInterruptibly(request);
        return requestSerial;
    }

    public final int send(NioMessage message) {
        byte status = (byte) 0x00;
        int requestSerial = serial.getAndIncrement();

        WritableSocketRequest request = new WritableSocketRequest(
                status,
                requestSerial,
                message
        );

        offerInterruptibly(request);
        return requestSerial;
    }

    public void broadcast(BytesConvertible message) {
        byte[] data = message.toBytes();

        for(SelectionKey selectionKey : contexts.keySet()) {
            ConnectionContext context = contexts.get(selectionKey);
            byte status = (byte) 0x00;
            context.offerRequest(new WritableSocketRequest(selectionKey, status, serial.getAndIncrement(), data));
        }

        interrupt();
    }

    public Set<SelectionKey> connectedNodes() {
        return contexts.keySet();
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
            }

            if(context.isReadCompleted()) {
                handler.handleResponse(context.fetchResponse());
            }
        }

        private void doWrite() throws Exception {
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
        private void handleDisconnect() throws Exception {
            contexts.remove(selectionKey);
            selectionKey.cancel();

            handler.handleDisconnect(selectionKey);

            logger.info("Disconnect from node [{}].", ((SocketChannel)selectionKey.channel()).getRemoteAddress());
        }
    }
}
