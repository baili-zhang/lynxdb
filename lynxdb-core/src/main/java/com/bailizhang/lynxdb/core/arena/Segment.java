package com.bailizhang.lynxdb.core.arena;

import com.bailizhang.lynxdb.core.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record Segment(
        ArenaBuffer parent,
        int offset,
        int length,
        ByteBuffer buffer
) {
    public static void deallocAll(Segment[] segments) {
        Arrays.stream(segments).forEach(Segment::dealloc);
    }

    public static Buffers buffers(Segment[] segments) {
        ByteBuffer[] buffers = Arrays.stream(segments)
                .map(Segment::buffer)
                .toArray(ByteBuffer[]::new);
        return new Buffers(buffers);
    }

    public void dealloc() {
        parent.dealloc(this);
    }
}
