package com.bailizhang.lynxdb.raft.common;

import java.nio.channels.SelectionKey;

public record RaftCommend(
        SelectionKey selectionKey,
        int serial,
        int index,
        byte[] data
) {
}
