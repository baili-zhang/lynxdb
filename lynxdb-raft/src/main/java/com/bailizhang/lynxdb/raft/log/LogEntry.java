package com.bailizhang.lynxdb.raft.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;

public record LogEntry(
        byte method,
        byte invalid,
        byte type,
        long dataBegin,
        int term,
        byte[] data
) implements BytesListConvertible {
    public static final int INDEX_BYTES_LENGTH = 15;

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        bytesList.appendRawByte(method);
        bytesList.appendRawByte(invalid);
        bytesList.appendRawByte(type);
        bytesList.appendRawLong(dataBegin);
        bytesList.appendRawInt(term);

        return bytesList;
    }

    public static LogEntry from(ByteBuffer buffer) {
        buffer.rewind();

        byte method = buffer.get();
        byte invalid = buffer.get();
        byte type = buffer.get();
        long dataBegin = buffer.getLong();
        int term = buffer.getInt();

        return new LogEntry(method, invalid, type, dataBegin, term, null);
    }

    public static LogEntry fromSocket(ByteBuffer buffer) {
        int term = buffer.getInt();
        byte[] data = BufferUtils.getRemaining(buffer);

        return new LogEntry(
                BufferUtils.EMPTY_BYTE,
                BufferUtils.EMPTY_BYTE,
                BufferUtils.EMPTY_BYTE,
                0,
                term,
                data
        );
    }
}
