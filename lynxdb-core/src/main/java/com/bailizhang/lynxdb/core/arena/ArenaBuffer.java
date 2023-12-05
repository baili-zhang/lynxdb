package com.bailizhang.lynxdb.core.arena;

import java.nio.ByteBuffer;
import java.util.BitSet;

public record ArenaBuffer (
        int bit,
        ByteBuffer buffer,
        BitSet bitSet
) {
    public static ArenaBuffer create(int bit, ByteBuffer buffer) {
        return new ArenaBuffer(bit, buffer, new BitSet(buffer.limit()));
    }

    public boolean isClear() {
        return bitSet.isEmpty();
    }

    public Segment alloc(int offset, int length) {
        bitSet.set(offset, offset + length, true);
        ByteBuffer segmentBuffer = buffer.slice(offset, length).asReadOnlyBuffer();

        return new Segment(this, offset, length, segmentBuffer);
    }

    public void dealloc(Segment segment) {
        int offset = segment.offset();
        int length = segment.length();
        bitSet.clear(offset, offset + length);
    }
}
