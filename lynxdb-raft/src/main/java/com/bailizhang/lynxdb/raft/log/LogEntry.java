package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public record LogEntry(
        LogIndex index,
        byte[] data
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        bytesList.appendRawInt(index.term());
        bytesList.appendRawLong(index.dataBegin());

        return bytesList;
    }
}
