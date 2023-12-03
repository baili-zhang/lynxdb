package com.bailizhang.lynxdb.socket.server;

import com.bailizhang.lynxdb.core.recorder.FlightDataRecorder;
import com.bailizhang.lynxdb.socket.client.CountDownSync;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.ReadableSocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.READ_DATA_FROM_SOCKET;
import static com.bailizhang.lynxdb.socket.measure.MeasureOptions.WRITE_DATA_TO_SOCKET;

public class IoEventHandler implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(IoEventHandler.class);

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
            SelectionKey key = channel.register(selector, SelectionKey.OP_READ);
            key.attach(new ReadableSocketRequest(key));
        }

        logger.info("Accept socket {} connect.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws Exception {
        ReadableSocketRequest request = (ReadableSocketRequest) selectionKey.attachment();

        FlightDataRecorder recorder = FlightDataRecorder.recorder();

        /* 从socket channel中读取数据 */
        try {
            if(recorder.isEnable()) {
                recorder.recordE(request::read, READ_DATA_FROM_SOCKET);
            } else {
                request.read();
            }
        } catch (SocketException e) {
            /* 取消掉selectionKey */
            selectionKey.cancel();
            latch.countDown();
            /* 打印客户端断开连接的日志 */
            return;
        }

        if (request.isReadCompleted()) {
            /* 是否断开连接 */
            if (!request.isKeepConnection()) {
                selectionKey.cancel();
                return;
            }
            /* 处理Socket请求 */
            handler.handleRequest(request);
            /* 未写回完成的请求数量加一 */
            context.increaseRequestCount();
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
                context.decreaseRequestCount();

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
                doAccept();
            } else if (selectionKey.isReadable()) {
                doRead();
            } else if (selectionKey.isWritable()) {
                doWrite();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
