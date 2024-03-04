/*
 * Copyright 2022-2024 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.server;

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.arena.Segment;
import com.bailizhang.lynxdb.socket.common.ArenaBufferManager;
import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public record SocketContext (
        SelectionKey selectionKey,
        ConcurrentLinkedQueue<WritableSocketResponse> responses,
        AtomicInteger unFinishedRequest,
        ArenaBufferManager arenaBufferManager
) {

    private static final Logger logger = LoggerFactory.getLogger(SocketContext.class);

    public static SocketContext create(SelectionKey selectionKey) {
        return new SocketContext(
                selectionKey,
                new ConcurrentLinkedQueue<>(),
                new AtomicInteger(0),
                new ArenaBufferManager()
        );
    }

    public void pollResponse() {
        responses.poll();
        int unFinished = unFinishedRequest.decrementAndGet();

        if(unFinished == 0) {
            selectionKey.interestOpsAnd(SelectionKey.OP_READ);
        } else if(unFinished < 0) {
            throw new RuntimeException();
        }
    }

    public WritableSocketResponse peekResponse() {
        return responses.peek();
    }

    public void offerResponse(WritableSocketResponse response) {
        responses.offer(response);
        selectionKey.interestOpsOr(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
    }

    public boolean responseQueueIsEmpty() {
        return responses.isEmpty();
    }

    public ArenaBuffer readableArenaBuffer() {
        return arenaBufferManager.readableArenaBuffer();
    }

    public List<SegmentSocketRequest> requests() {
        // 清除之前被释放的内存
        arenaBufferManager.clearFreeBuffers();

        List<SegmentSocketRequest> requests = new ArrayList<>();
        // 请求格式为 |长度|序列号|请求数据|
        while (true) {
            if(arenaBufferManager.notEnoughToRead(INT_LENGTH)) {
                break;
            }
            int length = arenaBufferManager.readInt(false);
            if(arenaBufferManager.notEnoughToRead(INT_LENGTH + length)) {
                break;
            }

            arenaBufferManager.incrementPosition(INT_LENGTH);
            int serial = arenaBufferManager.readInt(true);
            Segment[] data = arenaBufferManager.read(length - INT_LENGTH, true);
            requests.add(new SegmentSocketRequest(selectionKey, serial, data));
        }

        int count = unFinishedRequest.get();
        while(!unFinishedRequest.compareAndSet(count, count + requests.size())) {
            count = unFinishedRequest.get();
        }

        return requests;
    }

    public void destroy() {
        arenaBufferManager.dealloc();
    }
}
