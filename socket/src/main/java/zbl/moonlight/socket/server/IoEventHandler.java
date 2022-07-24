package zbl.moonlight.socket.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.socket.client.CountDownSync;
import zbl.moonlight.socket.interfaces.SocketServerHandler;
import zbl.moonlight.socket.request.ReadableSocketRequest;
import zbl.moonlight.socket.response.WritableSocketResponse;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class IoEventHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger("IoEventHandler");

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
            key.attach(new ReadableSocketRequest());
        }

        logger.info("Client {} has connected to server.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws Exception {
        ReadableSocketRequest request = (ReadableSocketRequest) selectionKey.attachment();
        SocketAddress address = ((SocketChannel) selectionKey.channel()).getRemoteAddress();

        /* 从socket channel中读取数据 */
        try {
            request.read();
        } catch (SocketException e) {
            /* 取消掉selectionKey */
            selectionKey.cancel();
            latch.countDown();
            /* 打印客户端断开连接的日志 */
            logger.info("Client {} has disconnected from server.", address);
            return;
        }

        if (request.isReadCompleted()) {
            /* 是否断开连接 */
            if (!request.isKeepConnection()) {
                selectionKey.cancel();
                logger.info("Client {} exit from server.", address);
                return;
            }
            /* 处理Socket请求 */
            handler.handleRequest(request);
            /* 未写回完成的请求数量加一 */
            context.increaseRequestCount();
            logger.debug("Request read completed.");
        }
    }

    /* 每次写多个请求 */
    private void doWrite() throws IOException {
        while (!context.responseQueueIsEmpty()) {
            WritableSocketResponse response = context.peekResponse();
            response.write();

            if (response.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                context.pollResponse();
                context.decreaseRequestCount();
                logger.debug("Send socket response: {} to client.", response);
            } else {
                /* 如果mdtpResponse没写完，说明写缓存已经写满了 */
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
