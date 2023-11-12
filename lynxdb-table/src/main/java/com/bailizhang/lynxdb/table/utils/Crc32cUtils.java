package com.bailizhang.lynxdb.table.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public interface Crc32cUtils {
    static void check(ByteBuffer buffer) {
        buffer.position(0);
        int crc32cOffset = buffer.capacity() - LONG_LENGTH;
        buffer.limit(crc32cOffset);

        CRC32C crc32C = new CRC32C();
        crc32C.update(buffer);
        long crc32cValue = crc32C.getValue();

        buffer.limit(buffer.capacity());
        long storeCrc32cValue = buffer.getLong(crc32cOffset);

        if(crc32cValue != storeCrc32cValue) {
            throw new RuntimeException();
        }
    }

    static long update(ByteBuffer buffer) {
        return 0L;
    }

    static long update(ByteBuffer buffer, int limit) {
        return 0L;
    }
}
