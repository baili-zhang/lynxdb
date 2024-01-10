/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.socket.client;

import com.bailizhang.lynxdb.socket.request.ByteBufferSocketRequest;
import com.bailizhang.lynxdb.socket.response.ReadableSocketResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ConnectionContext {
    private final SelectionKey selectionKey;
    private final ConcurrentLinkedQueue<ByteBufferSocketRequest> requests = new ConcurrentLinkedQueue<>();

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

    public void offerRequest(ByteBufferSocketRequest request) {
        if(lockRequestOffer) {
            throw new RuntimeException("Locked request offer, can not offer request.");
        }
        requests.offer(request);
        selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
    }

    public ByteBufferSocketRequest peekRequest() {
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
