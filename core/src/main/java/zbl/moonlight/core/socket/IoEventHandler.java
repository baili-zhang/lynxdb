package zbl.moonlight.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.core.protocol.common.Readable;
import zbl.moonlight.core.protocol.common.Writable;
import zbl.moonlight.core.protocol.common.ReadableEvent;
import zbl.moonlight.core.protocol.mdtp.MdtpMethod;
import zbl.moonlight.core.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public record IoEventHandler(SelectionKey selectionKey, CountDownLatch latch,
                             Selector selector,
                             RemainingWritableEvents remainingWritableEvents,
                             /* 下游执行器 */
                             Executable downstream,
                             /* 事件类型 */
                             EventType eventType,
                             Class<? extends Readable> readableClass) implements Runnable {

    private static final Logger logger = LogManager.getLogger("IoEventHandler");

    private void doAccept()
            throws IOException, NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, readableClass.getDeclaredConstructor().newInstance());
        }

        logger.info("Client {} has connected to server.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws IOException, InvocationTargetException, NoSuchMethodException,
            InstantiationException, IllegalAccessException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ReadableMdtpRequest mdtpRequest = (ReadableMdtpRequest) selectionKey.attachment();

        /* 从socket channel中读取数据 */
        try {
            mdtpRequest.read(socketChannel);
        } catch (SocketException e) {
            /* 取消掉selectionKey */
            selectionKey.cancel();
            latch.countDown();
            /* 打印客户端断开连接的日志 */
            SocketAddress address = ((SocketChannel) selectionKey.channel()).getRemoteAddress();
            logger.info("Client {} has disconnected from server.", address);
            return;
        }

        if (mdtpRequest.isReadCompleted()) {
            /* 如果为EXIT命令，直接将selectionKey取消掉 */
            if (mdtpRequest.method() == MdtpMethod.EXIT) {
                selectionKey.cancel();
                logger.info("A client exit connection.");
                return;
            }

            logger.debug("Received MDTP request is: {}", mdtpRequest);

            /* 未写回完成的请求数量加一 */
            remainingWritableEvents.increaseRequestCount();

            Event event = new Event(eventType, new ReadableEvent(selectionKey, mdtpRequest));
            downstream.offer(event);
        }
    }

    /* 每次写多个请求 */
    private void doWrite() throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        while (!remainingWritableEvents.isEmpty()) {
            Writable writable = remainingWritableEvents.peek();
            writable.write(socketChannel);

            if (writable.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                remainingWritableEvents.poll();
                remainingWritableEvents.decreaseRequestCount();
                logger.debug("Send MDTP response: {} to client.", writable);
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
