package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record DbValue(byte[] column, byte[] value) implements BytesListConvertible {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DbValue dbValue = (DbValue) o;
        return Arrays.equals(column, dbValue.column);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(column);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(column);
        bytesList.appendVarBytes(value);
        return bytesList;
    }

    @Override
    public String toString() {
        String template = "{ column: \"%s\", value: \"%s\" }";
        return String.format(
                template,
                G.I.toString(column),
                G.I.toString(value)
        );
    }

    public static DbValue from(ByteBuffer buffer) {
        byte[] column = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        return new DbValue(column, value);
    }
}
