package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.core.protocol.mdtp.MdtpMethod;
import zbl.moonlight.core.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.core.protocol.mdtp.WritableMdtpResponse;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public class IoEventHandler implements Runnable {

    private final SelectionKey selectionKey;
    private final CountDownLatch latch;
    private final Selector selector;
    private final RemainingResponseEvents remainingResponseEvents;
    private final EventBus eventBus;
    /* 事件分发的时候要查询是否是异步写日志 */
    private final Configuration config;

    public IoEventHandler(SelectionKey selectionKey,
                          CountDownLatch latch, Selector selector,
                          RemainingResponseEvents remainingResponseEvents) {
        this.selectionKey = selectionKey;
        this.latch = latch;
        this.selector = selector;
        this.remainingResponseEvents = remainingResponseEvents;
        ServerContext context = ServerContext.getInstance();
        this.eventBus = context.getEventBus();
        this.config = context.getConfiguration();
    }

    private static final Logger logger = LogManager.getLogger("IoEventHandler");

    private void doAccept()
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, new ReadableMdtpRequest());
        }

        logger.info("Client {} has connected to server.", channel.getRemoteAddress());
    }

    /* 每次读一个请求 */
    private void doRead() throws IOException {
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
            remainingResponseEvents.increaseRequestCount();

            /* 如果是同步写二进制日志 */
//            Event logEvent = new Event(EventType.BINARY_LOG_REQUEST, new MdtpRequestEvent(selectionKey, mdtpRequest));
            Event event = new Event(EventType.ENGINE_REQUEST, new MdtpRequestEvent(selectionKey, mdtpRequest));
            eventBus.offer(event);
//            if (config.getSyncWriteLog()) {
//                // 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求
//                if (mdtpRequest.method() == MdtpMethod.SET
//                        || mdtpRequest.method() == MdtpMethod.DELETE) {
//                    eventBus.offer(logEvent);
//                }
//                // 否则向事件总线发送客户端请求
//                else {
//                    eventBus.offer(event);
//                }
//            }
//            // 如果是异步写二进制日志
//            else {
//                // 向事件总线发送客户端请求
//                eventBus.offer(event);
//                // 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求
//                if (mdtpRequest.method() == MdtpMethod.SET
//                        || mdtpRequest.method() == MdtpMethod.DELETE) {
//                    eventBus.offer(logEvent);
//                }
//            }
        }
    }

    /* 每次写多个请求 */
    private void doWrite() throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        while (!remainingResponseEvents.isEmpty()) {
            WritableMdtpResponse response = remainingResponseEvents.peek();
            response.write(socketChannel);

            if (response.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                remainingResponseEvents.poll();
                remainingResponseEvents.decreaseRequestCount();
                logger.debug("Send MDTP response: {} to client.", response);
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
