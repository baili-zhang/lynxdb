package zbl.moonlight.socket.client;

import zbl.moonlight.socket.request.SocketRequest;
import zbl.moonlight.socket.request.WritableSocketRequest;
import zbl.moonlight.socket.response.ReadableSocketResponse;
import zbl.moonlight.socket.response.AbstractSocketResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConnectionContext {
    private final SelectionKey selectionKey;
    private final ConcurrentLinkedQueue<WritableSocketRequest> requests = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Object> attachments = new ConcurrentLinkedQueue<>();
    private final Object nullObject = new Object();

    private ReadableSocketResponse response;

    private AbstractSocketResponse abstractSocketResponse;

    /* TODO: exit 流程使用，lockRequestAdd 为 true 时，禁止向队列中添加请求 */
    private volatile boolean lockRequestOffer = false;

    public ConnectionContext(SelectionKey key) {
        selectionKey = key;
        response = new ReadableSocketResponse(selectionKey);
    }

    public void lockRequestOffer() {
        lockRequestOffer = true;
    }

    public void offerRequest(SocketRequest request) {
        if(lockRequestOffer) {
            throw new RuntimeException("Locked request offer, can not offer request.");
        }
        requests.offer(new WritableSocketRequest(request, selectionKey));
        Object attachment = request.attachment();
        attachments.offer(attachment == null ? nullObject : attachment);
        selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
    }

    public WritableSocketRequest peekRequest() {
        return requests.peek();
    }

    public void pollRequest() {
        requests.poll();
        if(requests.isEmpty()) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        }
    }

    public int sizeOfRequests() {
        return requests.size();
    }

    public boolean isReadCompleted() {
        if(response.isReadCompleted()) {
            /* 读取完成之后才设置 attachments */
            response.setAttachment(attachments.poll());
            abstractSocketResponse = response.socketResponse();
            this.response = new ReadableSocketResponse(selectionKey);
            return true;
        }
        return false;
    }

    public void read() throws IOException {
        response.read();
    }

    public AbstractSocketResponse socketResponse() {
        return abstractSocketResponse;
    }
}
