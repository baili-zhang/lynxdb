package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public record LogIndex(
        int extraDataLength,
        byte[] extraData,
        int dataBegin,
        int dataLength,
        long crc32c
) implements BytesListConvertible {

    public static final int FIXED_LENGTH = INT_LENGTH * 2 + LONG_LENGTH;

    public static LogIndex from(ByteBuffer buffer, int extraDataLength) {
        byte[] extraData = new byte[extraDataLength];
        buffer.get(extraData);
        int dataBegin = buffer.getInt();
        int dataLength = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(extraData);
        crc32C.update(dataBegin);
        crc32C.update(dataLength);

        long crc32CValue = crc32C.getValue();

        if(crc32CValue != crc32c) {
            throw new RuntimeException("Log Index data error");
        }

        return new LogIndex(
                extraDataLength,
                extraData,
                dataBegin,
                dataLength,
                crc32c
        );
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawBytes(extraData);
        bytesList.appendRawInt(dataBegin);
        bytesList.appendRawInt(dataLength);
        bytesList.appendRawLong(crc32c);
        return bytesList;
    }
}
