package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesArrayConvertible;
import com.bailizhang.lynxdb.core.common.BytesList;

import java.nio.ByteBuffer;
import java.util.List;

public record LogIndex(
        int extraDataLength,
        byte[] extraData,
        int dataBegin,
        int dataLength
) implements BytesArrayConvertible {

    public static final int FIXED_LENGTH = 8;

    public static LogIndex from(ByteBuffer buffer, int extraDataLength) {
        byte[] extraData = new byte[extraDataLength];
        buffer.get(extraData);
        int dataBegin = buffer.getInt();
        int dataLength = buffer.getInt();

        return new LogIndex(extraDataLength, extraData, dataBegin, dataLength);
    }

    @Override
    public List<byte[]> toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawBytes(extraData);
        bytesList.appendRawInt(dataBegin);
        bytesList.appendRawInt(dataLength);
        return bytesList.toBytesList();
    }
}
