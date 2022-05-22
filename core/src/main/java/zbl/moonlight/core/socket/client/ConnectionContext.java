package zbl.moonlight.core.socket.client;

import lombok.Getter;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.request.WritableSocketRequest;
import zbl.moonlight.core.socket.response.ReadableSocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class ConnectionContext {
    private final SelectionKey selectionKey;

    @Getter
    private final ConcurrentLinkedQueue<WritableSocketRequest> requests = new ConcurrentLinkedQueue<>();

    @Getter
    private final ConcurrentLinkedQueue<Object> attachments = new ConcurrentLinkedQueue<>();
    private final Object nullObject = new Object();

    @Getter
    private ReadableSocketResponse response;

    /* TODO: exit 流程使用，lockRequestAdd 为 true 时，禁止向队列中添加请求 */
    private volatile boolean lockRequestOffer = false;

    public ConnectionContext(SelectionKey key) {
        selectionKey = key;
    }

    public void newResponse() {
        Object attachment = attachments.poll();
        response = new ReadableSocketResponse(selectionKey, attachment == nullObject ? null : attachment);
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
        newResponse();
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
}
