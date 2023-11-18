package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record DataEntry(
        ByteBuffer[] data,
        long crc32c
) {

    public static DataEntry from(ByteBuffer[] data) {
        CRC32C dataCrc32C = new CRC32C();

        for(ByteBuffer buffer : data) {
            dataCrc32C.update(buffer);
        }
        long dataCrc32c = dataCrc32C.getValue();

        return new DataEntry(data, dataCrc32c);
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawBuffers(data);
        dataBlocks.appendRawLong(crc32c);
        return dataBlocks.toBuffers();
    }
}
