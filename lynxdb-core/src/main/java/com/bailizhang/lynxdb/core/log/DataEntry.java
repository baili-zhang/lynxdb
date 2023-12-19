package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;

public record DataEntry(
        ByteBuffer[] data,
        long crc32c
) {

    public static DataEntry from(ByteBuffer[] data) {
        long crc32c = Crc32cUtils.update(data);
        return new DataEntry(data, crc32c);
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawBuffers(data);
        dataBlocks.appendRawLong(crc32c);
        return dataBlocks.toBuffers();
    }
}
