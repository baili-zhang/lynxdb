package com.bailizhang.lynxdb.socket.common;

import com.bailizhang.lynxdb.core.arena.ArenaBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.exceptions.ReadCompletedException;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class ArenaBufferManager {
    private final List<ArenaBuffer> arenaBuffers = new ArrayList<>();
    private final ListIterator<ArenaBuffer> current = arenaBuffers.listIterator();
    private int position;

    public ArenaBuffer readableArenaBuffer() {
        if(arenaBuffers.isEmpty()) {
            ArenaBuffer newArenaBuffer = ArenaAllocator.alloc();
            arenaBuffers.addLast(newArenaBuffer);
            return newArenaBuffer;
        }

        ArenaBuffer arenaBuffer = arenaBuffers.getLast();
        if(!BufferUtils.isOver(arenaBuffer.buffer())) {
            return arenaBuffer;
        }

        ArenaBuffer newArenaBuffer = ArenaAllocator.alloc();
        arenaBuffers.addLast(newArenaBuffer);
        return newArenaBuffer;
    }

    public void dealloc() {
        arenaBuffers.forEach(ArenaAllocator::dealloc);
    }

    public ByteBuffer[] read(int length) throws ReadCompletedException {
        // 检查数据足够吗
        int idx = current.nextIndex();
        int size = arenaBuffers.size();

        int readableLength = 0, tempPosition = position, bufferCount = 0;
        for(int i = idx; i < size; i ++) {
            ByteBuffer buffer = arenaBuffers.get(i).buffer();
            int bufferPosition = buffer.position();
            if(bufferPosition == 0) {
                break;
            }
            if(bufferPosition == tempPosition) {
                continue;
            }
            readableLength += bufferPosition - tempPosition;
            bufferCount ++;
            tempPosition = 0;
        }

        if(readableLength < length) {
            throw new ReadCompletedException();
        }

        ByteBuffer[] buffers = new ByteBuffer[bufferCount];
        int i = 0;
        while (current.hasNext()) {
            position = 0;
            ArenaBuffer arenaBuffer = current.next();
            ByteBuffer buffer = arenaBuffer.buffer();

            if(position == buffer.position()) {
                continue;
            }

            int readLength = Math.min(buffer.position() - position, length);
            buffers[i] = buffer.slice(position, readLength).asReadOnlyBuffer();
            i ++;
            position += readLength;

            if(i >= bufferCount) {
                return buffers;
            }
        }

        throw new ReadCompletedException();
    }

    public int readInt() throws ReadCompletedException {
        ByteBuffer[] buffers = read(INT_LENGTH);
        if(buffers.length == 1) {
            return buffers[0].getInt();
        }

        ByteBuffer intBuffer = BufferUtils.intByteBuffer();
        for (ByteBuffer buffer : buffers) {
            intBuffer.put(buffer);
        }
        return intBuffer.getInt();
    }
}
