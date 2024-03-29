/*
 * Copyright 2022-2024 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.server;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.CountDownSync;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketServer extends Executor<WritableSocketResponse> {
    private static final Logger logger = LoggerFactory.getLogger(SocketServer.class);

    private final SocketServerConfig config;
    private final ThreadPoolExecutor executor;
    private final Selector selector;

    private SocketServerHandler handler;

    /* 响应的队列 */
    private final ConcurrentHashMap<SelectionKey, SocketContext> contexts
            = new ConcurrentHashMap<>();

    public void setHandler(SocketServerHandler handler) {
        this.handler = handler;
    }

    /* IO线程的线程工厂 */
    private class IoThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable runnable) {
            return new Thread(runnable, config.ioThreadNamePrefix() + threadNumber.getAndIncrement());
        }
    }

    public SocketServer(SocketServerConfig socketServerConfig) throws IOException {
        this.config = socketServerConfig;

        this.executor = new ThreadPoolExecutor(
                config.coreSize(),
                config.maxPoolSize(),
                config.keepAliveTime(),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.blockingQueueSize()),
                new IoThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );

        selector = Selector.open();

        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.bind(new InetSocketAddress(config.port()), config.backlog());
        serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    protected void doBeforeExecute() {
        handler.handleStartupCompleted();
    }

    @Override
    protected void execute() {
        try {
            selector.select();
            /* 如果线程被中断，则将线程中断位复位 */
            if(Thread.interrupted()) {
            }

            Set<SelectionKey> selectionKeys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = selectionKeys.iterator();
            CountDownSync latch = new CountDownSync(selectionKeys.size());

            synchronized (selector) {
                while (iterator.hasNext()) {
                    SelectionKey selectionKey = iterator.next();
                    try {
                        SocketContext context = contexts.get(selectionKey);
                        if(context == null) {
                            context = SocketContext.create(selectionKey);
                            contexts.put(selectionKey, context);
                        }
                        executor.execute(new IoEventHandler(context, latch, handler));
                    } catch (RejectedExecutionException ignored) {
                        latch.countDown();
                    }
                    iterator.remove();
                }
            }

            /* 从contexts中移除已经取消的selectionKey */
            for (SelectionKey key : contexts.keySet()) {
                if(!key.isValid()) {
                    SocketAddress address = ((SocketChannel)key.channel()).getRemoteAddress();

                    var context = contexts.remove(key);
                    context.destroy();

                    logger.info("Client {} is disconnect, Remove socket context", address);
                }
            }

            /* 从队列中拿出 SocketResponse */
            while (true) {
                WritableSocketResponse response = poll();
                if(response == null) {
                    break;
                }

                SelectionKey selectionKey = response.selectionKey();

                if(selectionKey == null) {
                    continue;
                }

                /* TODO: context 可能为 null，需要排查原因 */
                SocketContext context = contexts.get(selectionKey);
                context.offerResponse(response);
            }

            latch.await();
            handler.handleAfterLatchAwait();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
