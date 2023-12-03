package com.bailizhang.lynxdb.core.arena;

import java.nio.ByteBuffer;
import java.util.BitSet;

public class Arena {
    private final ByteBuffer buffer;
    private final BitSet bitSet;
    private final int allocSize;
    private final int bufferCount;
    private int maxClearBit;

    public Arena(int mainBufferSize, int allocBufferSize) {
        allocSize = allocBufferSize;

        buffer = ByteBuffer.allocateDirect(mainBufferSize);
        bufferCount = mainBufferSize/ allocBufferSize;

        if(mainBufferSize % allocBufferSize != 0) {
            throw new RuntimeException();
        }

        bitSet = new BitSet(bufferCount);
        maxClearBit = 0;
    }

    public synchronized ArenaBuffer alloc() {
        // 因为 alloc 内存并不是频繁发生，所以直接使用 cardinality() 计算
        if(bitSet.cardinality() == bufferCount) {
            // TODO 内存分配满后的处理
            throw new RuntimeException();
        }

        int nextClearBit = bitSet.nextClearBit(maxClearBit);
        maxClearBit = (nextClearBit + 1) % bufferCount;

        ByteBuffer allocBuffer = buffer.slice(nextClearBit * allocSize, allocSize);
        return new ArenaBuffer(nextClearBit, allocBuffer);
    }

    public synchronized void dealloc(ArenaBuffer arenaBuffer) {
        int bit = arenaBuffer.bit();
        bitSet.clear(bit);
    }
}
