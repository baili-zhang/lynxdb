package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public record LogIndex(
        byte deleteFlag,
        int dataBegin,
        int dataLength,
        long crc32c
) implements BytesListConvertible {

    public static final int FIXED_LENGTH = BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH;

    public static LogIndex from(ByteBuffer buffer) {
        byte flag = buffer.get();
        int dataBegin = buffer.getInt();
        int dataLength = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(dataBegin);
        crc32C.update(dataLength);

        long crc32CValue = crc32C.getValue();

        if(crc32CValue != crc32c) {
            throw new RuntimeException("Log Index data error");
        }

        return new LogIndex(
                flag,
                dataBegin,
                dataLength,
                crc32c
        );
    }

    public static LogIndex from(
            byte deleteFlag,
            int dataBegin,
            int dataLength
    ) {
        CRC32C indexCrc32C = new CRC32C();
        indexCrc32C.update(new byte[]{deleteFlag});
        indexCrc32C.update(dataBegin);
        indexCrc32C.update(dataLength);
        long indexCrc32c = indexCrc32C.getValue();

        return new LogIndex(
                deleteFlag,
                dataBegin,
                dataLength,
                indexCrc32c
        );
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawInt(dataBegin);
        bytesList.appendRawInt(dataLength);
        bytesList.appendRawLong(crc32c);
        return bytesList;
    }
}
