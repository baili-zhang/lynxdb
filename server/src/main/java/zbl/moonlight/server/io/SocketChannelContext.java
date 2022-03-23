package zbl.moonlight.server.io;

import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.protocol.mdtp.WritableMdtpResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SocketChannelContext {
    private final SelectionKey selectionKey;
    /* 响应队列 */
    private final ConcurrentLinkedQueue<MdtpResponseEvent> responses = new ConcurrentLinkedQueue<>();
    /* 正在处理的请求数量，需要同步处理 */
    private int requestCount = 0;

    SocketChannelContext(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public MdtpResponseEvent poll() {
        return responses.poll();
    }

    public MdtpResponseEvent peek() {
        return responses.peek();
    }

    public void offer(MdtpResponseEvent response) {
        responses.offer(response);
    }

    public boolean isEmpty() {
        return responses.isEmpty();
    }

    public void increaseRequestCount() {
        if(requestCount == 0) {
            /* 设置新的请求对象 */
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
        selectionKey.attach(new WritableMdtpResponse());
        requestCount ++;
    }

    public void decreaseRequestCount() {
        requestCount --;
        if(requestCount == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }
}
