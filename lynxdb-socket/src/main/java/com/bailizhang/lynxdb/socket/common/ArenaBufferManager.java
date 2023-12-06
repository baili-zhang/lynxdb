package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.arena.Segment;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.exceptions.ReadCompletedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class ArenaBufferManager {
    private final List<ArenaBuffer> arenaBuffers = new LinkedList<>();
    private int position = 0;

    public ArenaBuffer readableArenaBuffer() {
        if(arenaBuffers.isEmpty()) {
            ArenaBuffer newArenaBuffer = ArenaAllocator.alloc();
            arenaBuffers.addLast(newArenaBuffer);
            return newArenaBuffer;
        }

        ArenaBuffer arenaBuffer = arenaBuffers.getLast();
        if(arenaBuffer.notFull()) {
            return arenaBuffer;
        }

        ArenaBuffer newArenaBuffer = ArenaAllocator.alloc();
        arenaBuffers.addLast(newArenaBuffer);
        return newArenaBuffer;
    }

    public void dealloc() {
        arenaBuffers.forEach(ArenaAllocator::dealloc);
    }

    public Segment[] read(int length) throws ReadCompletedException {
        if(arenaBuffers.isEmpty()) {
            throw new RuntimeException();
        }

        int size = arenaBuffers.size();
        int total = arenaBuffers.getLast().position()
                + (size - 1) * ArenaAllocator.ARENA_BUFFER_SIZE;

        // 检查数据足够吗
        if(position + length > total) {
            throw new ReadCompletedException();
        }

        int idx = position / ArenaAllocator.ARENA_BUFFER_SIZE;

        List<Segment> segments = new ArrayList<>();
        for(int i = idx; i < size && length > 0; i ++) {
            ArenaBuffer arenaBuffer = arenaBuffers.get(i);
            int bufferPosition = arenaBuffer.position();

            int readPosition = position % ArenaAllocator.ARENA_BUFFER_SIZE;
            int readLength = Math.min(length, bufferPosition - readPosition);

            Segment segment = arenaBuffer.alloc(readPosition, readLength);

            segments.add(segment);
            length -= readLength;
            position += readLength;
        }

        return segments.toArray(Segment[]::new);
    }

    public int readInt() throws ReadCompletedException {
        Segment[] segments = read(INT_LENGTH);
        if(segments.length == 1) {
            // 返还分配的内存
            Segment.deallocAll(segments);
            return segments[0].buffer().getInt();
        }

        ByteBuffer intBuffer = BufferUtils.intByteBuffer();
        for (Segment segment : segments) {
            intBuffer.put(segment.buffer());
        }
        // 返还分配的内存
        Segment.deallocAll(segments);
        return intBuffer.rewind().getInt();
    }

    public void clearFreeBuffers() {
        ArenaBuffer arenaBuffer;
        while (!arenaBuffers.isEmpty()
                && (arenaBuffer = arenaBuffers.getFirst()).isClear()) {
            ArenaAllocator.dealloc(arenaBuffer);
            arenaBuffers.removeFirst();
            position -= ArenaAllocator.ARENA_BUFFER_SIZE;
        }
    }
}
