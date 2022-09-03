package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.channels.SelectionKey;

public record RaftLogEntry (
        SelectionKey selectionKey,
        int serial,
        int term,
        byte type,
        byte[] command
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        return null;
    }
}
