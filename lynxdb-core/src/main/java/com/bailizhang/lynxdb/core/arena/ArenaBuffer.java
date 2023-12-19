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
