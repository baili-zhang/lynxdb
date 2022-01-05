package zbl.moonlight.server.io;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.exception.EventTypeException;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.*;

/**
 * 实现MDTP协议的服务器
 */
public class MdtpSocketServer extends Executor<Event<?>> {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");
    @Getter
    private final String NAME = "MdtpSocketServer";

    private final Configuration config;
    private final ThreadPoolExecutor executor;

    /* 事件总线的对象和线程 */
    private final EventBus eventBus;
    private final Thread eventBusThread;

    private final ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap
            = new ConcurrentHashMap<>();

    public MdtpSocketServer(Configuration config, EventBus eventBus,
                            Thread eventBusThread) {
        super(eventBus, eventBusThread);
        this.config = config;
        this.executor = new ThreadPoolExecutor(config.getIoThreadCorePoolSize(),
                config.getIoThreadMaxPoolSize(),
                30,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),
                Executors.defaultThreadFactory(),
                new ThreadPoolExecutor.DiscardPolicy());
        this.eventBus = eventBus;
        this.eventBusThread = eventBusThread;
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(config.getPort()));
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while (true) {
                selector.select();
                Set<SelectionKey> selectionKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectionKeys.iterator();
                CountDownLatch latch = new CountDownLatch(selectionKeys.size());

                synchronized (selector) {
                    while (iterator.hasNext()) {
                        SelectionKey selectionKey = iterator.next();
                        executor.execute(new IoEventHandler(selectionKey, latch, selector,
                                eventBus, eventBusThread, responsesMap));
                        iterator.remove();
                    }
                }

                /* 从队列中拿出响应事件放入responsesMap中 */
                while (true) {
                    Event event = pollIn();
                    if(event == null) {
                        break;
                    }

                    Object response = event.getValue();
                    if(!(response instanceof MdtpResponse)) {
                        throw new EventTypeException("value is not an instance of MdtpResponse.");
                    }
                    SelectionKey selectionKey = event.getSelectionKey();
                    ConcurrentLinkedQueue<MdtpResponse> responses = responsesMap.get(selectionKey);
                    if(responses == null) {
                        responses = new ConcurrentLinkedQueue<>();
                        responsesMap.put(selectionKey, responses);
                    }
                    responses.offer((MdtpResponse) response);
                }
                latch.await();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
