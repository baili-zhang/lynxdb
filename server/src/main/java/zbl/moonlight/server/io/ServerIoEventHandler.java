package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.protocol.MdtpMethod;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public record ServerIoEventHandler(SelectionKey selectionKey,
                                   CountDownLatch latch, Selector selector,
                                   EventBus eventBus,
                                   ConcurrentHashMap<SelectionKey, SocketChannelContext> contexts,
                                   Configuration config) implements Runnable {
    private static final Logger logger = LogManager.getLogger("ServerIoEventHandler");

    private void doAccept(SelectionKey selectionKey)
            throws IOException {
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) selectionKey.channel();
        SocketChannel channel = serverSocketChannel.accept();
        channel.configureBlocking(false);

        synchronized (selector) {
            channel.register(selector, SelectionKey.OP_READ, new MdtpRequest());
        }
    }

    /* 每次读一个请求 */
    private void doRead(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        MdtpRequest mdtpRequest = (MdtpRequest) selectionKey.attachment();
        mdtpRequest.read(socketChannel);

        if (mdtpRequest.isReadCompleted()) {
            /* 如果为EXIT命令，直接将selectionKey取消掉 */
            if (mdtpRequest.getMethod() == MdtpMethod.EXIT) {
                contexts.remove(selectionKey);
                selectionKey.cancel();
                logger.info("A client exit connection.");
                return;
            }
            SocketChannelContext context = contexts.get(selectionKey);
            /* 如果没有上下文对象，则添加上下文对象 */
            if (context == null) {
                /* TODO:Socket连接关闭时需要删除对应的上下文，不然会导致内存溢出 */
                context = new SocketChannelContext(selectionKey);
                contexts.put(selectionKey, context);
            }
            context.increaseRequestCount();

            /* 如果是同步写二进制日志 */
            if (config.getSyncWriteLog()) {
                /* 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求 */
                if (mdtpRequest.getMethod() == MdtpMethod.SET
                        || mdtpRequest.getMethod() == MdtpMethod.DELETE) {
                    eventBus.offer(new Event(EventType.BINARY_LOG_REQUEST, selectionKey, mdtpRequest.duplicate()));
                }
                /* 否则向事件总线发送客户端请求 */
                else {
                    eventBus.offer(new Event(EventType.CLIENT_REQUEST, selectionKey, mdtpRequest.duplicate()));
                }
            }
            /* 如果是异步写二进制日志 */
            else {
                /* 向事件总线发送客户端请求 */
                eventBus.offer(new Event(EventType.CLIENT_REQUEST, selectionKey, mdtpRequest.duplicate()));
                /* 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求 */
                if (mdtpRequest.getMethod() == MdtpMethod.SET
                        || mdtpRequest.getMethod() == MdtpMethod.DELETE) {
                    eventBus.offer(new Event(EventType.BINARY_LOG_REQUEST, selectionKey, mdtpRequest.duplicate()));
                }
            }
        }
    }

    /* 每次写多个请求 */
    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        SocketChannelContext context = contexts.get(selectionKey);

        while (!context.isEmpty()) {
            MdtpResponse mdtpResponse = context.peek();
            if (mdtpResponse != null) {
                mdtpResponse.write(socketChannel);
                if (mdtpResponse.isWriteCompleted()) {
                    /* 从队列首部移除已经写完的响应 */
                    context.poll();
                    context.decreaseRequestCount();
                } else {
                    /* 如果mdtpResponse没写完，说明写缓存已经写满了 */
                    break;
                }
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
