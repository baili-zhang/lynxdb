package com.bailizhang.lynxdb.lsmtree.entry;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record IndexEntry(
        byte flag, // 是否被删除
        int begin, // 顺序查找不需要，二分查找需要这个字段
        int length,
        long crc32c
) implements BytesListConvertible {

    public static IndexEntry from(byte flag, int begin, int length) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(begin);
        crc32C.update(length);

        long crc32c = crc32C.getValue();

        return new IndexEntry(flag, begin, length, crc32c);
    }

    public static IndexEntry from(ByteBuffer buffer) {
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

        return new IndexEntry(flag, begin, length, crc32c);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(flag);
        bytesList.appendRawInt(begin);
        bytesList.appendRawInt(length);
        bytesList.appendRawLong(crc32c);

        return bytesList;
    }
}
