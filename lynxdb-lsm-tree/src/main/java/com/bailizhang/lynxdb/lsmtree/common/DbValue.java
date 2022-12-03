package com.bailizhang.lynxdb.lsmtree.common;

import java.util.Arrays;

public record DbValue(byte[] column, byte[] value) {

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
}
