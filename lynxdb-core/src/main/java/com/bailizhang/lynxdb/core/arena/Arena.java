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

package com.bailizhang.lynxdb.core.arena;

import com.bailizhang.lynxdb.core.arena.exceptions.ArenaOverflowException;

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

    public synchronized ArenaBuffer alloc() throws ArenaOverflowException {
        // 因为 alloc 内存并不是频繁发生，所以直接使用 cardinality() 计算
        if(bitSet.cardinality() == bufferCount) {
            throw new ArenaOverflowException();
        }

        int nextClearBit = bitSet.nextClearBit(maxClearBit);
        bitSet.set(nextClearBit);
        maxClearBit = (nextClearBit + 1) % bufferCount;

        ByteBuffer allocBuffer = buffer.slice(nextClearBit * allocSize, allocSize);
        return new ArenaBuffer(nextClearBit, allocBuffer);
    }

    public synchronized void dealloc(ArenaBuffer arenaBuffer) {
        int bit = arenaBuffer.bit();
        bitSet.clear(bit);
    }
}
