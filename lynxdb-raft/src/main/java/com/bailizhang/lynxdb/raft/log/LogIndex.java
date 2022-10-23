package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesConvertible;

import java.nio.ByteBuffer;

public record LogIndex(int term, long dataBegin, int dataLength)
        implements BytesConvertible {

    public static final int BYTES_LENGTH = 16;

    public static LogIndex from(ByteBuffer buffer) {
        int term = buffer.getInt();
        long dataBegin = buffer.getLong();
        int dataLength = buffer.getInt();

        return new LogIndex(term, dataBegin, dataLength);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(BYTES_LENGTH);
        return buffer.putInt(term).putLong(dataBegin).putInt(dataLength).array();
    }
}
