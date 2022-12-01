package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;

public record DbIndex(
        DbKey key,
        int valueGlobalIndex
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.append(key);
        bytesList.appendRawInt(valueGlobalIndex);
        return bytesList;
    }

    public static DbIndex from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();

        DbKey dbKey = new DbKey(key, column);
        return new DbIndex(dbKey, valueGlobalIndex);
    }
}
