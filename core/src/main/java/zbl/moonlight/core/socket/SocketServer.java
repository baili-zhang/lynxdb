package zbl.moonlight.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.exception.UnSupportedEventTypeException;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.protocol.common.Readable;
import zbl.moonlight.core.protocol.common.WritableEvent;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SocketServer extends Executor {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");

    private final int port;
    private final int backlog;
    private final Executable downstream;
    private final EventType eventType;
    private final ThreadPoolExecutor executor;
    private final String ioThreadNamePrefix;
    private final Class<? extends Readable> readableClass;

    /* 响应的队列 */
    private final ConcurrentHashMap<SelectionKey, RemainingWritableEvents> responses
            = new ConcurrentHashMap<>();

    /* IO线程的线程工厂 */
    private class IoThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable r) {
            return new Thread(r, ioThreadNamePrefix + threadNumber.getAndIncrement());
        }
    }

    public SocketServer(int coreSize, int maxPoolSize, int keepAliveTime,
                        int blockingQueueSize, int port, int backlog,
                        Executable downstream,
                        EventType eventType,
                        String ioThreadNamePrefix,
                        Class<? extends Readable> readableClass) {
        this.executor = new ThreadPoolExecutor(coreSize,
                maxPoolSize,
                keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(blockingQueueSize),
                new IoThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
        this.port = port;
        this.backlog = backlog;
        this.downstream = downstream;
        this.eventType = eventType;
        this.ioThreadNamePrefix = ioThreadNamePrefix;
        this.readableClass = readableClass;
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(port), backlog);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                CountDownLatch latch = new CountDownLatch(selectionKeys.size());

                /* 对Set进行迭代，不同步处理的话，可能会出问题 */
                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        try {
                            RemainingWritableEvents events = responses.get(selectionKey);
                            if(events == null) {
                                events = new RemainingWritableEvents(selectionKey, readableClass);
                                responses.put(selectionKey, events);
                            }
                            executor.execute(new IoEventHandler(selectionKey, latch, selector,
                                    events, downstream, eventType, readableClass));
                        } catch (RejectedExecutionException e) {
                            latch.countDown();
                            e.printStackTrace();
                        }
                        iterator.remove();
                    }
                }

                /* 从responses中移除已经取消的selectionKey */
                for (SelectionKey key : responses.keySet()) {
                    if(!key.isValid()) {
                        SocketAddress address = ((SocketChannel)key.channel()).getRemoteAddress();
                        responses.remove(key);
                        logger.info("Delete response queue from [responses](map) of {}.", address);
                    }
                }

                /* 从队列中拿出响应事件 */
                while (true) {
                    Event event = poll();
                    if(event == null) {
                        break;
                    }

                    if(!(event.value() instanceof WritableEvent writableEvent)) {
                        throw new UnSupportedEventTypeException("event.value() is not an instance of MdtpResponse.");
                    }
                    SelectionKey selectionKey = writableEvent.selectionKey();
                    RemainingWritableEvents events = responses.get(selectionKey);
                    events.offer(writableEvent.writable());
                }

                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
