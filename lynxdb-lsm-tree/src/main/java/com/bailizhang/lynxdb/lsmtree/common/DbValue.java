package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

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
}
