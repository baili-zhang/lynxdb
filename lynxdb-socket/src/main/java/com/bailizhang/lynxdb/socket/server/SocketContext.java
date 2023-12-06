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

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public record SocketContext (
        SelectionKey selectionKey,
        ConcurrentLinkedQueue<WritableSocketResponse> responses,
        ArenaBufferManager arenaBufferManager
) {

    private static final Logger logger = LoggerFactory.getLogger(SocketContext.class);

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

        return requests;
    }

    public void destroy() {
        selectionKey.cancel();
        arenaBufferManager.dealloc();
    }
}
