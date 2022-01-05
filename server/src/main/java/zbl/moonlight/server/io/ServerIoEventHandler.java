package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
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
    private static final Logger logger = LogManager.getLogger("IoEventHandler");

    private final SelectionKey selectionKey;
    private final CountDownLatch latch;
    private final Selector selector;
    private final EventBus eventBus;
    private final Thread notifiedThread;
    private final ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap;

    public ServerIoEventHandler(SelectionKey selectionKey, CountDownLatch latch, Selector selector,
                                EventBus eventBus,
                                Thread notifiedThread,
                                ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap) {
        this.selectionKey = selectionKey;
        this.latch = latch;
        this.selector = selector;
        this.eventBus = eventBus;
        this.notifiedThread = notifiedThread;
        this.responsesMap = responsesMap;
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

    /**
     * 每次读一个请求
     * @param selectionKey
     * @throws IOException
     */
    private void doRead(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        MdtpRequest mdtpRequest = (MdtpRequest) selectionKey.attachment();
        mdtpRequest.read(socketChannel);

        if(mdtpRequest.isReadCompleted()) {
            DynamicByteBuffer value = mdtpRequest.getValue();
            if(value != null) {
                value.flip();
            }

            /* 将读完的请求加入到请求队列中 */
            eventBus.offer(new Event<>(EventType.CLIENT_REQUEST, selectionKey, mdtpRequest));
            if(Thread.State.TIMED_WAITING.equals(notifiedThread.getState())) {
                notifiedThread.interrupt();
            }
            /* 设置新的请求对象 */
            selectionKey.attach(new MdtpRequest());
            /* selectionKey添加写事件监听 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
            logger.info("received mdtp request: " + mdtpRequest + ".");
        }
    }

    /**
     * 每次写多个响应
     * @param selectionKey
     * @throws IOException
     */
    private void doWrite(SelectionKey selectionKey) throws IOException {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ConcurrentLinkedQueue<MdtpResponse> responses = responsesMap.get(selectionKey);
        if(responses == null) {
            throw new IOException("response queue is null");
        }

        while(!responses.isEmpty()) {
            MdtpResponse mdtpResponse = responses.peek();
            if(mdtpResponse != null) {
                mdtpResponse.write(socketChannel);
                if(mdtpResponse.isWriteCompleted()) {
                    /* 从队列首部移除已经写完的响应 */
                    responses.poll();
                    logger.info("one mdtp response is written to client.");
                } else {
                    /* 如果mdtpResponse没写完，说明写缓存已经写满了 */
                    break;
                }
            }
        }

        /* 如果response队列为空，则取消写事件监听 */
        if(responses.isEmpty()) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
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
