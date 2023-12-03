package com.bailizhang.lynxdb.core.arena;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Arena {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

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

    public ArenaBuffer alloc() {
        if(bitSet.cardinality() == bufferCount) {
            // TODO 内存分配满后的处理
            throw new RuntimeException();
        }

        int nextClearBit;
        lock.readLock().lock();

        try {
            nextClearBit = bitSet.nextClearBit(maxClearBit);
            maxClearBit = nextClearBit + 1;
        } finally {
            lock.readLock().unlock();
        }


        ByteBuffer allocBuffer = buffer.slice(nextClearBit * allocSize, allocSize);
        return new ArenaBuffer(nextClearBit, allocBuffer);
    }

    public void dealloc(ArenaBuffer arenaBuffer) {
        int bit = arenaBuffer.bit();

        lock.writeLock().lock();

        try {
            bitSet.clear(bit);
        } finally {
            lock.writeLock().unlock();
        }
    }
}
