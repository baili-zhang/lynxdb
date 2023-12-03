package com.bailizhang.lynxdb.socket.server;

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.socket.common.ArenaBufferManager;
import com.bailizhang.lynxdb.socket.exceptions.ReadCompletedException;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public record SocketContext (
        SelectionKey selectionKey,
        ConcurrentLinkedQueue<WritableSocketResponse> responses,
        ArenaBufferManager arenaBufferManager
) {

    public static SocketContext create(SelectionKey selectionKey) {
        return new SocketContext(
                selectionKey,
                new ConcurrentLinkedQueue<>(),
                new ArenaBufferManager()
        );
    }

    public void pollResponse() {
        responses.poll();
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

    public List<SocketRequest> requests() {
        List<SocketRequest> requests = new ArrayList<>();
        // 请求格式为 |长度|序列号|请求数据|
        try {
            while (true) {
                int length = arenaBufferManager.readInt();
                int serial = arenaBufferManager.readInt();
                ByteBuffer[] data = arenaBufferManager.read(length - INT_LENGTH);
                requests.add(new SocketRequest(selectionKey, serial, data));
            }
        } catch (ReadCompletedException ignored) {}

        return requests;
    }

    public void destroy() {
        selectionKey.cancel();
        arenaBufferManager.dealloc();
    }
}
