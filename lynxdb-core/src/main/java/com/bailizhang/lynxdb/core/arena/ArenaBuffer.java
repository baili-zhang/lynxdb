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

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.BitSet;

public class ArenaBuffer {
    private final int bit;
    private final ByteBuffer buffer;
    private final BitSet bitSet;

    public ArenaBuffer(int bit, ByteBuffer buffer) {
        this.bit = bit;
        this.buffer = buffer;
        this.bitSet = new BitSet(buffer.limit());
    }

    public int bit() {
        return bit;
    }

    public int position() {
        return buffer.position();
    }

    public synchronized void read(SocketChannel channel) throws IOException {
        int oldPosition = buffer.position();
        channel.read(buffer);
        int newPosition = buffer.position();

        if(oldPosition == newPosition) {
            return;
        }

        bitSet.set(oldPosition, newPosition, true);
    }

    public synchronized boolean notFull() {
        return BufferUtils.isNotOver(buffer);
    }

    public synchronized boolean isClear() {
        return bitSet.isEmpty();
    }

    public synchronized Segment alloc(int offset, int length) {
        bitSet.set(offset, offset + length, true);
        ByteBuffer segmentBuffer = buffer.slice(offset, length).asReadOnlyBuffer();
        return new Segment(this, offset, length, segmentBuffer);
    }

    public synchronized void dealloc(Segment segment) {
        int offset = segment.offset();
        int length = segment.length();
        bitSet.clear(offset, offset + length);
    }
}
