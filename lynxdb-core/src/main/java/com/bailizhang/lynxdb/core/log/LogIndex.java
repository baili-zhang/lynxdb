package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesConvertible;

import java.nio.ByteBuffer;

public record LogIndex(
        int extraDataLength,
        byte[] extraData,
        long dataBegin,
        int dataLength
) implements BytesConvertible {

    public static final int FIXED_LENGTH = 12;

    public static LogIndex from(ByteBuffer buffer, int extraDataLength) {
        byte[] extraData = new byte[extraDataLength];
        buffer.get(extraData);
        long dataBegin = buffer.getLong();
        int dataLength = buffer.getInt();

        return new LogIndex(extraDataLength, extraData, dataBegin, dataLength);
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(extraDataLength + FIXED_LENGTH);
        return buffer.put(extraData).putLong(dataBegin).putInt(dataLength).array();
    }
}
