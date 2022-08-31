package com.bailizhang.lynxdb.socket.client;

import com.bailizhang.lynxdb.socket.request.WritableSocketRequest;
import com.bailizhang.lynxdb.socket.response.ReadableSocketResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConnectionContext {
    private final SelectionKey selectionKey;
    private final ConcurrentLinkedQueue<WritableSocketRequest> requests = new ConcurrentLinkedQueue<>();

    private ReadableSocketResponse response;

    /* TODO: exit 流程使用，lockRequestAdd 为 true 时，禁止向队列中添加请求 */
    private volatile boolean lockRequestOffer = false;

    public ConnectionContext(SelectionKey key) {
        selectionKey = key;
        response = new ReadableSocketResponse(selectionKey);
    }

    public void lockRequestOffer() {
        lockRequestOffer = true;
    }

    public void offerRequest(WritableSocketRequest request) {
        if(lockRequestOffer) {
            throw new RuntimeException("Locked request offer, can not offer request.");
        }
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

    public int sizeOfRequests() {
        return requests.size();
    }

    public boolean isReadCompleted() {
        return response.isReadCompleted();
    }

    public ReadableSocketResponse fetchResponse() {
        ReadableSocketResponse completed = response;
        response = new ReadableSocketResponse(selectionKey);
        return completed;
    }

    public void read() throws IOException {
        response.read();
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }
}
