package zbl.moonlight.core.socket;

import zbl.moonlight.core.protocol.common.Readable;
import zbl.moonlight.core.protocol.common.Writable;

import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/* 保存还未写回客户端的响应，用一个队列来维护 */
public class RemainingWritableEvents {
    private final SelectionKey selectionKey;
    private final Class<? extends Readable> readableClass;
    /* 响应队列 */
    private final ConcurrentLinkedQueue<Writable> responses = new ConcurrentLinkedQueue<>();
    /* 正在处理的请求数量 */
    private final AtomicInteger requestCount = new AtomicInteger(0);

    public RemainingWritableEvents(SelectionKey selectionKey, Class<? extends Readable> readableClass) {
        this.selectionKey = selectionKey;
        this.readableClass = readableClass;
    }

    public void poll() {
        responses.poll();
    }

    public Writable peek() {
        return responses.peek();
    }

    public void offer(Writable response) {
        responses.offer(response);
    }

    public boolean isEmpty() {
        return responses.isEmpty();
    }

    public void increaseRequestCount() throws NoSuchMethodException, InvocationTargetException,
            InstantiationException, IllegalAccessException {
        if(requestCount.get() == 0) {
            /* 设置新的request对象 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
        selectionKey.attach(readableClass.getDeclaredConstructor().newInstance());
        requestCount.getAndIncrement();
    }

    public void decreaseRequestCount() {
        requestCount.getAndDecrement();
        if(requestCount.get() == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }
}
