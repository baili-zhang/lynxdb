/*
 * Copyright 2021-2024 Baili Zhang.
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

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.recorder.FlightDataRecorder;
import com.bailizhang.lynxdb.core.recorder.IRunnable;
import com.bailizhang.lynxdb.socket.client.CountDownSync;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.READ_DATA_FROM_SOCKET;
import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.WRITE_DATA_TO_SOCKET;

public class IoEventHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IoEventHandler.class);

    private static final int MAX_ARENA_BUFFERS_TO_READ = 10;

    private final SocketContext context;
    private final CountDownSync latch;
    private final SocketServerHandler handler;
    private final SelectionKey selectionKey;
    private final Selector selector;

    IoEventHandler(SocketContext socketContext, CountDownSync countDownLatch, SocketServerHandler socketServerHandler) {
        context = socketContext;
        latch = countDownLatch;
        handler = socketServerHandler;
        selectionKey = context.selectionKey();
        selector = selectionKey.selector();
    }

    private void doAccept() throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ);
        }

        logger.info("Accept socket {} connect.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws Exception {
        FlightDataRecorder recorder = FlightDataRecorder.recorder();

        IRunnable read = () -> {
            SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

            int times = MAX_ARENA_BUFFERS_TO_READ;
            while ((times --) > 0) {
                ArenaBuffer arenaBuffer = context.readableArenaBuffer();

                arenaBuffer.read(socketChannel);

                if(arenaBuffer.notFull()) {
                    return;
                }
            }
        };

        try {
            recorder.recordE(read, READ_DATA_FROM_SOCKET);
        } catch (IOException e) {
            selectionKey.cancel();
            return;
        }

        List<SegmentSocketRequest> requests = context.requests();
        for(SegmentSocketRequest request : requests) {
            handler.handleRequest(request);
        }
    }

    /* 每次写多个请求 */
    private void doWrite() throws Exception {
        while (!context.responseQueueIsEmpty()) {
            WritableSocketResponse response = context.peekResponse();

            FlightDataRecorder recorder = FlightDataRecorder.recorder();

            if(recorder.isEnable()) {
                recorder.recordE(response::write, WRITE_DATA_TO_SOCKET);
            } else {
                response.write();
            }

            if (response.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                context.pollResponse();
                logger.trace("Write response completed to client, response: {}", response);
            } else {
                break;
            }
        }
    }

    @Override
    public void run() {
        try {
            if (selectionKey.isAcceptable()) {
                logger.info("isAcceptable");
                doAccept();
            }

            if (selectionKey.isWritable()) {
                logger.info("isWritable");
                doWrite();
            }

            if (selectionKey.isReadable()) {
                logger.info("isReadable");
                doRead();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
