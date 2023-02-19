package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Objects;

public record DbIndex(
        KeyEntry dbKey,
        int valueGlobalIndex
) implements BytesListConvertible {

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.append(dbKey);
        bytesList.appendRawInt(valueGlobalIndex);
        return bytesList;
    }

    public static DbIndex from(byte flag, ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();

        KeyEntry dbKey = new KeyEntry(key, column, flag);
        return new DbIndex(dbKey, valueGlobalIndex);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbIndex dbIndex = (DbIndex) o;
        return Objects.equals(dbKey, dbIndex.dbKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dbKey);
    }
}
