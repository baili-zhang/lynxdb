package com.bailizhang.lynxdb.socket.request;

import com.bailizhang.lynxdb.core.arena.Segment;

import java.nio.channels.SelectionKey;

public record SegmentSocketRequest(
        SelectionKey selectionKey,
        int serial,
        Segment[] data
) {
}
