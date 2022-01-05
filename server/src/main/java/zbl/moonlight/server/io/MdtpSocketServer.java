package zbl.moonlight.server.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 实现MDTP协议的服务器
 */
public class MdtpSocketServer extends Executor<MdtpRequest, MdtpResponse> {
    private static final Logger logger = LogManager.getLogger("MdtpSocketServer");

    private final int port;
    private final ThreadPoolExecutor executor;
    private final EventBus eventBus;
    private final ConcurrentHashMap<SelectionKey, ConcurrentLinkedQueue<MdtpResponse>> responsesMap
            = new ConcurrentHashMap<>();

    public MdtpSocketServer(int port, ThreadPoolExecutor executor, Thread notifiedThread,
                            EventBus eventBus) {
        super(notifiedThread);
        this.port = port;
        this.executor = executor;
        this.eventBus = eventBus;
    }

    @Override
    public void run() {
        try {
            Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.bind(new InetSocketAddress(port));
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
                                eventBus, notifiedThread, responsesMap));
                        iterator.remove();
                    }
                }

                /* 从事件总线中拿响应事件放入responsesMap中 */
                while (true) {
                    Event event = eventBus.poll(EventType.CLIENT_RESPONSE);
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
