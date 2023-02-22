package com.bailizhang.lynxdb.lsmtree.entry;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.zip.CRC32C;

public record IndexEntry(
        byte flag, // 是否被删除
        int begin,
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
        // TODO
        throw new UnsupportedOperationException();
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
