package com.bailizhang.lynxdb.raft.common;

import java.nio.channels.SelectionKey;

public record AppliableLogEntry(
        SelectionKey selectionKey,
        int serial,
        byte type,
        byte[] data
) {
    public static final byte CLIENT_COMMAND = (byte) 0x01;
    public static final byte MEMBER_CHANGE = (byte) 0x02;
}
