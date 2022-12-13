package com.bailizhang.lynxdb.server.engine.affect;

import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record AffectKey (
        byte[] key,
        byte[] columnFamily
) {
    public static AffectKey from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);


        return new AffectKey(key, columnFamily);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AffectKey affectKey = (AffectKey) o;
        return Arrays.equals(key, affectKey.key)
                && Arrays.equals(columnFamily, affectKey.columnFamily);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(columnFamily);
        return result;
    }
}
