package zbl.moonlight.core.socket.client;

import lombok.Getter;
import zbl.moonlight.core.socket.request.WritableSocketRequest;
import zbl.moonlight.core.socket.response.ReadableSocketResponse;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class ConnectionContext {
    private final SelectionKey selectionKey;
    private final ServerNode serverNode;

    @Getter
    private final ConcurrentLinkedQueue<WritableSocketRequest> requests = new ConcurrentLinkedQueue<>();

    @Getter
    private ReadableSocketResponse response;

    public ConnectionContext(SelectionKey key) {
        selectionKey = key;
        serverNode = (ServerNode) key.attachment();
        response = new ReadableSocketResponse(serverNode);
    }

    public void replaceResponse() {
        response = new ReadableSocketResponse(serverNode);
    }

    public void offerRequest(WritableSocketRequest request) {
        requests.offer(request);
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
}
