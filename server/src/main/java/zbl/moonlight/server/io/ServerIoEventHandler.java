package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.server.protocol.mdtp.MdtpMethod;
import zbl.moonlight.server.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.server.protocol.mdtp.WritableMdtpResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;

public class ServerIoEventHandler implements Runnable {

    private final SelectionKey selectionKey;
    private final CountDownLatch latch;
    private final Selector selector;
    private final RemainingResponseEvents remainingResponseEvents;
    private final EventBus eventBus;
    private final Configuration config;

    public ServerIoEventHandler(SelectionKey selectionKey,
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

    private static final Logger logger = LogManager.getLogger("ServerIoEventHandler");

    private void doAccept(SelectionKey selectionKey)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, new ReadableMdtpRequest());
        }
    }

    /* 每次读一个请求 */
    private void doRead(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ReadableMdtpRequest mdtpRequest = (ReadableMdtpRequest) selectionKey.attachment();
        mdtpRequest.read(socketChannel);

        if (mdtpRequest.isReadCompleted()) {
            /* 如果为EXIT命令，直接将selectionKey取消掉 */
            if (mdtpRequest.method() == MdtpMethod.EXIT) {
                selectionKey.cancel();
                logger.info("A client exit connection.");
                return;
            }

            /* 未写回完成的请求数量加一 */
            remainingResponseEvents.increaseRequestCount();

            /* 如果是同步写二进制日志 */
            Event logEvent = new Event(EventType.BINARY_LOG_REQUEST, new MdtpRequestEvent(selectionKey, mdtpRequest));
            Event event = new Event(EventType.ENGINE_REQUEST, new MdtpRequestEvent(selectionKey, mdtpRequest));
            if (config.getSyncWriteLog()) {
                /* 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求 */
                if (mdtpRequest.method() == MdtpMethod.SET
                        || mdtpRequest.method() == MdtpMethod.DELETE) {
                    eventBus.offer(logEvent);
                }
                /* 否则向事件总线发送客户端请求 */
                else {
                    eventBus.offer(event);
                }
            }
            /* 如果是异步写二进制日志 */
            else {
                /* 向事件总线发送客户端请求 */
                eventBus.offer(event);
                /* 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求 */
                if (mdtpRequest.method() == MdtpMethod.SET
                        || mdtpRequest.method() == MdtpMethod.DELETE) {
                    eventBus.offer(logEvent);
                }
            }
        }
    }

    /* 每次写多个请求 */
    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        while (!remainingResponseEvents.isEmpty()) {
            WritableMdtpResponse response = remainingResponseEvents.peek();
            response.write(socketChannel);

            if (response.isWriteCompleted()) {
                /* 从队列首部移除已经写完的响应 */
                remainingResponseEvents.poll();
                remainingResponseEvents.decreaseRequestCount();
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
                doAccept(selectionKey);
            } else if (selectionKey.isReadable()) {
                doRead(selectionKey);
            } else if (selectionKey.isWritable()) {
                doWrite(selectionKey);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
    }
}
