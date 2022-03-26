package zbl.moonlight.core.socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.exception.UnSupportedEventTypeException;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.protocol.nio.NioWriter;

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

    private final SocketServerConfig config;
    private final ThreadPoolExecutor executor;

    /* 响应的队列 */
    private final ConcurrentHashMap<SelectionKey, RemainingNioWriter> responses
            = new ConcurrentHashMap<>();

    /* IO线程的线程工厂 */
    private class IoThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        public Thread newThread(Runnable r) {
            return new Thread(r, config.ioThreadNamePrefix() + threadNumber.getAndIncrement());
        }
    }

    public SocketServer(SocketServerConfig socketServerConfig) {
        this.config = socketServerConfig;
        this.executor = new ThreadPoolExecutor(config.coreSize(),
                config.maxPoolSize(),
                config.keepAliveTime(),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.blockingQueueSize()),
                new IoThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(config.port()), config.backlog());
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
                            RemainingNioWriter events = responses.get(selectionKey);
                            if(events == null) {
                                events = new RemainingNioWriter(selectionKey, config.schemaClass());
                                responses.put(selectionKey, events);
                            }
                            executor.execute(new IoEventHandler(selectionKey, latch, selector,
                                    events, config.downstream(), config.eventType(), config.schemaClass()));
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

                    if(!(event.value() instanceof NioWriter writer)) {
                        throw new UnSupportedEventTypeException("event.value() is not an instance of MdtpResponse.");
                    }
                    SelectionKey selectionKey = writer.getSelectionKey();
                    RemainingNioWriter events = responses.get(selectionKey);
                    events.offer(writer);
                }

                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
