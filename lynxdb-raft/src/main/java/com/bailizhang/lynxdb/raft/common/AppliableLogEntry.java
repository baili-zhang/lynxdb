package com.bailizhang.lynxdb.raft.common;

import java.nio.channels.SelectionKey;

public record AppliableLogEntry(
        SelectionKey selectionKey,
        int serial,
        byte type,
        byte[] data
) {
}
