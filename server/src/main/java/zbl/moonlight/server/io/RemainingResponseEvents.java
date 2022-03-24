package zbl.moonlight.server.io;

import zbl.moonlight.core.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.core.protocol.mdtp.WritableMdtpResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/* 保存还未写回客户端的响应，用一个队列来维护 */
public class RemainingResponseEvents {
    private final SelectionKey selectionKey;
    /* 响应队列 */
    private final ConcurrentLinkedQueue<WritableMdtpResponse> responses = new ConcurrentLinkedQueue<>();
    /* 正在处理的请求数量 */
    private final AtomicInteger requestCount = new AtomicInteger(0);

    RemainingResponseEvents(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public void poll() {
        responses.poll();
    }

    public WritableMdtpResponse peek() {
        return responses.peek();
    }

    public void offer(WritableMdtpResponse response) {
        responses.offer(response);
    }

    public boolean isEmpty() {
        return responses.isEmpty();
    }

    public void increaseRequestCount() {
        if(requestCount.get() == 0) {
            /* 设置新的request对象 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
        selectionKey.attach(new ReadableMdtpRequest());
        requestCount.getAndIncrement();
    }

    public void decreaseRequestCount() {
        requestCount.getAndDecrement();
        if(requestCount.get() == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }
}
