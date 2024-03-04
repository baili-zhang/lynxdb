/*
 * Copyright 2023 Baili Zhang.
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

package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.arena.Segment;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class ArenaBufferManager {
    private final List<ArenaBuffer> arenaBuffers = new LinkedList<>();
    private volatile int position = 0;

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

    public boolean notEnoughToRead(int length) {
        if(arenaBuffers.isEmpty()) {
            throw new RuntimeException();
        }

        int size = arenaBuffers.size();
        int total = arenaBuffers.getLast().position()
                + (size - 1) * ArenaAllocator.ARENA_BUFFER_SIZE;

        return position + length > total;
    }

    public Segment[] read(int length, boolean isPositionChange) {
        // 检查数据足够吗
        if(notEnoughToRead(length)) {
            throw new RuntimeException();
        }

        int idx = position / ArenaAllocator.ARENA_BUFFER_SIZE;
        int size = arenaBuffers.size();

        List<Segment> segments = new ArrayList<>();
        int tempPosition = position, remainingLength = length;
        for(int i = idx; i < size && remainingLength > 0; i ++) {
            ArenaBuffer arenaBuffer = arenaBuffers.get(i);
            int bufferPosition = arenaBuffer.position();

            int readPosition = tempPosition % ArenaAllocator.ARENA_BUFFER_SIZE;
            int readLength = Math.min(remainingLength, bufferPosition - readPosition);

            Segment segment = arenaBuffer.alloc(readPosition, readLength);
            segments.add(segment);

            remainingLength -= readLength;
            tempPosition += readLength;
        }

        if(isPositionChange) {
            position += length;
        }

        return segments.toArray(Segment[]::new);
    }

    public int readInt(boolean isPositionChange) {
        Segment[] segments = read(INT_LENGTH, isPositionChange);
        if(segments.length == 1) {
            int value = segments[0].buffer().getInt();
            // 返还分配的内存
            Segment.deallocAll(segments);
            return value;
        }

        ByteBuffer intBuffer = BufferUtils.intByteBuffer();
        for (Segment segment : segments) {
            intBuffer.put(segment.buffer());
        }
        // 返还分配的内存
        Segment.deallocAll(segments);
        return intBuffer.rewind().getInt();
    }

    public void incrementPosition(int length) {
        position += length;
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
