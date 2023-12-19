package com.bailizhang.lynxdb.table.lsmtree.sstable;

import com.bailizhang.lynxdb.core.common.DataBlocks;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record SecondIndexEntry(
        byte flag, // 是否删除
        int begin, // 顺序查找不需要，二分查找需要这个字段
        int length,
        long crc32c
) {

    public static SecondIndexEntry from(byte flag, int begin, int length) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(begin);
        crc32C.update(length);

        long crc32c = crc32C.getValue();

        return new SecondIndexEntry(flag, begin, length, crc32c);
    }

    public static SecondIndexEntry from(ByteBuffer buffer) {
        byte flag = buffer.get();
        int begin = buffer.getInt();
        int length = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(begin);
        crc32C.update(length);

        if(crc32c != crc32C.getValue()) {
            throw new RuntimeException("Data Error");
        }

        return new SecondIndexEntry(flag, begin, length, crc32c);
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(flag);
        dataBlocks.appendRawInt(begin);
        dataBlocks.appendRawInt(length);
        dataBlocks.appendRawLong(crc32c);

        return dataBlocks.toBuffers();
    }
}
