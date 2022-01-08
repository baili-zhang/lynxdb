package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class ServerIoEventHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger("ServerIoEventHandler");

    private final SelectionKey selectionKey;
    private final CountDownLatch latch;
    private final Selector selector;
    private final EventBus eventBus;
    private final ConcurrentHashMap<SelectionKey, SocketChannelContext> contexts;

    public ServerIoEventHandler(SelectionKey selectionKey, CountDownLatch latch, Selector selector,
                                EventBus eventBus,
                                ConcurrentHashMap<SelectionKey, SocketChannelContext> contexts) {
        this.selectionKey = selectionKey;
        this.latch = latch;
        this.selector = selector;
        this.eventBus = eventBus;
        this.contexts = contexts;
    }

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

        if(mdtpRequest.isReadCompleted()) {
            DynamicByteBuffer value = mdtpRequest.getValue();
            if(value != null) {
                value.flip();
            }

            SocketChannelContext context = contexts.get(selectionKey);
            /* 如果没有上下文对象，则添加上下文对象 */
            if(context == null) {
                /* TODO:Socket连接关闭时需要删除对应的上下文，不然会导致内存溢出 */
                context = new SocketChannelContext(selectionKey);
                contexts.put(selectionKey, context);
            }
            context.increaseRequestCount();

            /* 向事件总线发送客户端请求 */
            eventBus.offer(new Event<>(EventType.CLIENT_REQUEST, selectionKey, mdtpRequest));
            /* 如果是SET或者DELETE请求，则向事件总线发送二进制日志请求 */
            if(mdtpRequest.getMethod() == MdtpMethod.SET
                    || mdtpRequest.getMethod() == MdtpMethod.DELETE) {
                eventBus.offer(new Event<>(EventType.BINARY_LOG_REQUEST, selectionKey, mdtpRequest));
            }
            // logger.info("received mdtp request: " + mdtpRequest + ".");
        }
    }

    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        SocketChannelContext context = contexts.get(selectionKey);

        while(!context.isEmpty()) {
            MdtpResponse mdtpResponse = context.peek();
            if(mdtpResponse != null) {
                mdtpResponse.write(socketChannel);
                if(mdtpResponse.isWriteCompleted()) {
                    /* 从队列首部移除已经写完的响应 */
                    context.poll();
                    context.decreaseRequestCount();
                    // logger.info("one mdtp response is written to client.");
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
            if(selectionKey.isAcceptable()) {
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
