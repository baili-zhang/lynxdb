/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.socket.client;

import com.bailizhang.lynxdb.core.common.CheckThreadSafety;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.recorder.FlightDataRecorder;
import com.bailizhang.lynxdb.core.recorder.IRunnable;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.SocketUtils;
import com.bailizhang.lynxdb.socket.common.NioMessage;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.request.ByteBufferSocketRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.CLIENT_READ_DATA_FROM_SOCKET;
import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.CLIENT_WRITE_DATA_TO_SOCKET;


public class SocketClient extends Executor<ByteBufferSocketRequest> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SocketClient.class);

    private final static int DEFAULT_KEEP_ALIVE_TIME = 30;
    private final static int DEFAULT_CAPACITY = 200;
    private final static int DEFAULT_CORE_POOL_SIZE = 5;
    private final static int DEFAULT_MAX_POOL_SIZE = 10;
    private final static int DEFAULT_CONNECT_TIMES = 3;

    private final Object setLock = new Object();
    private final AtomicInteger serial = new AtomicInteger(0);

    private final ConcurrentHashMap<ServerNode, SelectionKey> connected
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SelectionKey, ConnectionContext> contexts
            = new ConcurrentHashMap<>();

    /**
     * 处理连接的 futures
     */
    private final ConcurrentHashMap<SelectionKey, LynxDbFuture<SelectionKey>> connectFutureMap
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
        executor = new ThreadPoolExecutor(
                DEFAULT_CORE_POOL_SIZE,
                DEFAULT_MAX_POOL_SIZE,
                DEFAULT_KEEP_ALIVE_TIME,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(DEFAULT_CAPACITY),
                new ClientThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @CheckThreadSafety
    public synchronized LynxDbFuture<SelectionKey> connect(ServerNode node) throws IOException {
        SocketAddress address = new InetSocketAddress(node.host(), node.port());
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        socketChannel.connect(address);

        SelectionKey selectionKey;

        synchronized (setLock) {
            selectionKey = socketChannel.register(
                    selector,
                    SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE,
                    node
            );
        }

        LynxDbFuture<SelectionKey> future = new LynxDbFuture<>();

        contexts.put(selectionKey, new ConnectionContext(selectionKey));
        connectFutureMap.put(selectionKey, future);

        interrupt();
        return future;
    }

    public synchronized void disconnect(SelectionKey selectionKey) {
        if(contexts.containsKey(selectionKey)) {
            ConnectionContext context = contexts.get(selectionKey);
            SelectionKey key = context.selectionKey();
            /* 将主动退出的 selectionKey 加入 exitKeys，等待请求 */
            synchronized (exitKeys) {
                exitKeys.add(key);
            }
            // TODO: 忘了这里需要处理什么了
            context.lockRequestOffer();

            logger.info("Disconnect socket server {}.", SocketUtils.address(selectionKey));
            interrupt();
        }
    }

    public boolean isConnected(ServerNode member) {
        return connected.containsKey(member);
    }

    @Override
    public void offerInterruptibly(ByteBufferSocketRequest request) {
        SelectionKey selectionKey = request.selectionKey();

        ConnectionContext context = contexts.get(request.selectionKey());
        if(context == null) {
            if(!selectionKey.isValid()) {
                return;
            }
            throw new RuntimeException();
        }

        context.offerRequest(request);
        interrupt();
    }

    public final int send(NioMessage message) {
        SelectionKey selectionKey = message.selectionKey();
        return send(selectionKey, message.toBuffers());
    }

    public final int send(SelectionKey selectionKey, ByteBuffer[] data) {
        if(!selectionKey.isValid()) {
            throw new CancelledKeyException();
        }

        int requestSerial = serial.getAndIncrement();

        ByteBufferSocketRequest request = new ByteBufferSocketRequest(
                selectionKey,
                requestSerial,
                data
        );

        handler.handleBeforeSend(selectionKey, requestSerial);

        offerInterruptibly(request);
        return requestSerial;
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

    @Override
    public void close() {
        shutdown();
    }

    @Override
    protected void doAfterShutdown() {
        executor.shutdown();

        logger.info("Socket client has shutdown.");
    }

    /* IO线程的线程工厂 */
    private static class ClientThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        ClientThreadFactory() {
            namePrefix = "lynxdb-client-";
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
            try {
                if (selectionKey.isConnectable()) {
                    doConnect();
                } else if (selectionKey.isReadable()) {
                    doRead();
                } else if (selectionKey.isWritable()) {
                    doWrite();
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
                int times = DEFAULT_CONNECT_TIMES;
                while (times > 0) {
                    if(socketChannel.finishConnect()) {
                        break;
                    }
                    times --;
                }

                if(socketChannel.isConnected()) {
                    /* 处理连接成功 */
                    selectionKey.interestOpsAnd(SelectionKey.OP_READ);
                    LynxDbFuture<SelectionKey> future = connectFutureMap.remove(selectionKey);
                    future.value(selectionKey);

                    ServerNode node = (ServerNode) selectionKey.attachment();

                    connected.put(node, selectionKey);
                    handler.handleConnected(selectionKey);
                }
            } catch (ConnectException e) {
                // 删除 selectionKey 的上下文
                contexts.remove(selectionKey);

                LynxDbFuture<SelectionKey> future = connectFutureMap.remove(selectionKey);
                future.cancel(false);

                try {
                    handler.handleConnectFailure(selectionKey);
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        private void doRead() throws Exception {
            ConnectionContext context = contexts.get(selectionKey);

            FlightDataRecorder recorder = FlightDataRecorder.recorder();

            try {
                if(recorder.isEnable()) {
                    recorder.recordE(context::read, CLIENT_READ_DATA_FROM_SOCKET);
                } else {
                    context.read();
                }
            } catch (SocketException e) {
                handleDisconnect();
            }

            if(context.isReadCompleted()) {
                handler.handleResponse(context.fetchResponse());
            }
        }

        private void doWrite() throws Exception {
            ConnectionContext context = contexts.get(selectionKey);
            ByteBufferSocketRequest request = context.peekRequest();
            ByteBuffer[] writeData = request.toBuffers();

            FlightDataRecorder recorder = FlightDataRecorder.recorder();

            IRunnable write = () -> {
                SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
                socketChannel.write(writeData);
            };

            recorder.recordE(write, CLIENT_WRITE_DATA_TO_SOCKET);

            if(BufferUtils.isOver(writeData)) {
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
            ServerNode node = (ServerNode) selectionKey.attachment();
            connected.remove(node);

            contexts.remove(selectionKey);
            selectionKey.cancel();

            handler.handleDisconnect(selectionKey);
        }
    }
}
