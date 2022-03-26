package zbl.moonlight.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.protocol.nio.NioWriter;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public record IoEventHandler(SelectionKey selectionKey, CountDownLatch latch,
                             Selector selector,
                             RemainingNioWriter remainingNioWriter,
                             /* 下游执行器 */
                             Executable downstream,
                             /* 事件类型 */
                             EventType eventType,
                             Class<? extends Parsable> schemaClass) implements Runnable {

    private static final Logger logger = LogManager.getLogger("IoEventHandler");

    private void doAccept() throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, new NioReader(schemaClass, selectionKey));
        }

        logger.info("Client {} has connected to server.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws IOException {
        NioReader reader = (NioReader) selectionKey.attachment();

        /* 从socket channel中读取数据 */
        try {
            reader.read();
        } catch (SocketException e) {
            /* 取消掉selectionKey */
            selectionKey.cancel();
            latch.countDown();
            /* 打印客户端断开连接的日志 */
            SocketAddress address = ((SocketChannel) selectionKey.channel()).getRemoteAddress();
            logger.info("Client {} has disconnected from server.", address);
            return;
        }

        if (reader.isReadCompleted()) {
            /* 是否断开连接 */
            if (!reader.isKeepConnection()) {
                selectionKey.cancel();
                logger.info("A client exit connection.");
                return;
            }

            logger.debug("Received MDTP request is: {}", reader);

            /* 未写回完成的请求数量加一 */
            remainingNioWriter.increaseRequestCount();

            Event event = new Event(eventType, reader);
            downstream.offer(event);
        }
    }

    /* 每次写多个请求 */
    private void doWrite() throws IOException {
        while (!remainingNioWriter.isEmpty()) {
            NioWriter writer = remainingNioWriter.peek();
            writer.write();

            if (writer.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                remainingNioWriter.poll();
                remainingNioWriter.decreaseRequestCount();
                logger.debug("Send MDTP response: {} to client.", writer);
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
