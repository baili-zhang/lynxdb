package zbl.moonlight.core.socket;

import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/* 保存还未写回客户端的响应，用一个队列来维护 */
public class RemainingNioWriter {
    private final SelectionKey selectionKey;
    private final Class<? extends Parsable> schemaClass;
    /* 响应队列 */
    private final ConcurrentLinkedQueue<NioWriter> responses = new ConcurrentLinkedQueue<>();
    /* 正在处理的请求数量 */
    private final AtomicInteger requestCount = new AtomicInteger(0);

    public RemainingNioWriter(SelectionKey selectionKey, Class<? extends Parsable> schemaClass) {
        this.selectionKey = selectionKey;
        this.schemaClass = schemaClass;
    }

    public void poll() {
        responses.poll();
    }

    public NioWriter peek() {
        return responses.peek();
    }

    public void offer(NioWriter writer) {
        responses.offer(writer);
    }

    public boolean isEmpty() {
        return responses.isEmpty();
    }

    public void increaseRequestCount() {
        if(requestCount.get() == 0) {
            /* 设置新的request对象 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
        selectionKey.attach(new NioReader(schemaClass, selectionKey));
        requestCount.getAndIncrement();
    }

    public void decreaseRequestCount() {
        requestCount.getAndDecrement();
        if(requestCount.get() == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }
}
