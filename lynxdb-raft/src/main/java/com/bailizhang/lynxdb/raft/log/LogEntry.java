package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;

public record LogEntry(
        byte method,
        byte invalid,
        byte type,
        long dataBegin,
        byte[] data
) implements BytesListConvertible {
    public static final int INDEX_BYTES_LENGTH = 11;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        bytesList.appendRawByte(method);
        bytesList.appendRawByte(invalid);
        bytesList.appendRawByte(type);
        bytesList.appendRawLong(dataBegin);

        return bytesList;
    }

    public static LogEntry from(ByteBuffer buffer) {
        buffer.rewind();

        byte method = buffer.get();
        byte invalid = buffer.get();
        byte type = buffer.get();
        long dataBegin = buffer.getLong();

        return new LogEntry(method, invalid, type, dataBegin, null);
    }
}
