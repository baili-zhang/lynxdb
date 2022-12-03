package com.bailizhang.lynxdb.server.engine.affect;

import java.util.Arrays;

public record AffectKey (
        byte[] key,
        byte[] columnFamily,
        byte[] column
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AffectKey affectKey = (AffectKey) o;
        return Arrays.equals(key, affectKey.key) && Arrays.equals(columnFamily, affectKey.columnFamily) && Arrays.equals(column, affectKey.column);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(columnFamily);
        result = 31 * result + Arrays.hashCode(column);
        return result;
    }
}
