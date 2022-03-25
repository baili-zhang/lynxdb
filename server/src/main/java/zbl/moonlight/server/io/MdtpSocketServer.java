package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.exception.UnSupportedEventTypeException;
import zbl.moonlight.core.executor.Executor;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class MdtpSocketServer extends Executor {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");

    private final Configuration config;
    private final ThreadPoolExecutor executor;

    /* 响应的队列 */
    private final ConcurrentHashMap<SelectionKey, RemainingWritableEvents> responses
            = new ConcurrentHashMap<>();

    /* IO线程的线程工厂 */
    private static class IoThreadFactory implements ThreadFactory {
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        IoThreadFactory() {
            namePrefix = "IO" + "-Thread-";
        }

        public Thread newThread(Runnable r) {
            return new Thread(r, namePrefix + threadNumber.getAndIncrement());
        }
    }

    public MdtpSocketServer() {
        ServerContext context = ServerContext.getInstance();
        config = context.getConfiguration();
        this.executor = new ThreadPoolExecutor(config.getIoThreadCorePoolSize(),
                config.getIoThreadMaxPoolSize(),
                config.getIoThreadKeepAliveTime(),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.getIoThreadBlockingQueueSize()),
                new IoThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(config.getPort()), config.getBacklog());
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
                                events = new RemainingWritableEvents(selectionKey);
                                responses.put(selectionKey, events);
                            }
                            executor.execute(new IoEventHandler(selectionKey, latch, selector, events));
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

                    if(!(event.value() instanceof MdtpResponseEvent responseEvent)) {
                        throw new UnSupportedEventTypeException("event.value() is not an instance of MdtpResponse.");
                    }
                    SelectionKey selectionKey = responseEvent.selectionKey();
                    RemainingWritableEvents events = responses.get(selectionKey);
                    events.offer(responseEvent.response());
                }

                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
