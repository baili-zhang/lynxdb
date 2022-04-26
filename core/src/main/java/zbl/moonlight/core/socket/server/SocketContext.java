package zbl.moonlight.core.socket.server;

import zbl.moonlight.core.socket.request.ReadableSocketRequest;
import zbl.moonlight.core.socket.response.WritableSocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/* 保存还未写回客户端的响应，用一个队列来维护 */
public class SocketContext {
    private final SelectionKey selectionKey;
    /* 响应队列 */
    private final ConcurrentLinkedQueue<WritableSocketResponse> responses = new ConcurrentLinkedQueue<>();
    /* 正在处理的请求数量 */
    private final AtomicInteger requestCount = new AtomicInteger(0);

    public SocketContext(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public void pollResponse() {
        responses.poll();
    }

    public WritableSocketResponse peekResponse() {
        return responses.peek();
    }

    public void offerResponse(WritableSocketResponse response) {
        responses.offer(response);
    }

    public boolean responseQueueIsEmpty() {
        return responses.isEmpty();
    }

    public void increaseRequestCount() {
        if(requestCount.get() == 0) {
            /* 设置新的request对象 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
        selectionKey.attach(new ReadableSocketRequest(selectionKey));
        requestCount.getAndIncrement();
    }

    public void decreaseRequestCount() {
        requestCount.getAndDecrement();
        if(requestCount.get() == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }
}
