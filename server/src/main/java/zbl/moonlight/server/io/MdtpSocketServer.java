package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.exception.UnSupportedEventTypeException;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.mdtp.WritableMdtpResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

public class MdtpSocketServer extends Executor {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");

    private final Configuration config;
    private final ThreadPoolExecutor executor;

    /* 事件总线的对象和线程 */
    private final EventBus eventBus;

    private final ConcurrentHashMap<SelectionKey, SocketChannelContext> contexts
            = new ConcurrentHashMap<>();

    public MdtpSocketServer() {
        ServerContext context = ServerContext.getInstance();
        eventBus = context.getEventBus();
        config = context.getConfiguration();
        this.executor = new ThreadPoolExecutor(config.getIoThreadCorePoolSize(),
                config.getIoThreadMaxPoolSize(),
                config.getIoThreadKeepAliveTime(),
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(config.getIoThreadBlockingQueueSize()),
                /* TODO:需要自定义线程工厂 */
                Executors.defaultThreadFactory(),
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

                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        try {
                            executor.execute(new ServerIoEventHandler(selectionKey, latch, selector,
                                    eventBus, contexts, config));
                        } catch (RejectedExecutionException e) {
                            e.printStackTrace();
                            latch.countDown();
                        }
                        iterator.remove();
                    }
                }

                /* 从队列中拿出响应事件放入responsesMap中 */
                while (true) {
                    Event event = poll();
                    if(event == null) {
                        break;
                    }

                    Object value = event.value();
                    if(!(value instanceof MdtpResponseEvent)) {
                        throw new UnSupportedEventTypeException("event.value() is not an instance of MdtpResponse.");
                    }
                    MdtpResponseEvent response = (MdtpResponseEvent) value;
                    SelectionKey selectionKey = response.selectionKey();
                    SocketChannelContext context = contexts.get(selectionKey);
                    if(context == null) {
                        throw new IOException("SocketChannelContext object not found.");
                    }
                    context.offer(response);
                }

                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
