package com.bailizhang.lynxdb.core.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

public interface Crc32cUtils {
    static void check(ByteBuffer buffer) {
        buffer.rewind();
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
        buffer.rewind();
    }

    static long update(ByteBuffer buffer) {
        // TODO
        return 0L;
    }

    static long update(ByteBuffer buffer, int limit) {
        // TODO
        return 0L;
    }

    static long update(ByteBuffer[] buffers) {
        CRC32C crc32C = new CRC32C();
        for(ByteBuffer buffer : buffers) {
            int position = buffer.position();
            crc32C.update(buffer);
            buffer.position(position);
        }
        return crc32C.getValue();
    }
}
