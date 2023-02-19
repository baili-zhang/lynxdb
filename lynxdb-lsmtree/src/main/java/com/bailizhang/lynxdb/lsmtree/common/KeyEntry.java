package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

public record KeyEntry(byte[] key, byte flag, long crc32)
        implements Comparable<KeyEntry>, BytesListConvertible {

    public static final byte EXISTED = (byte) 0x01;
    public static final byte DELETED = (byte) 0x02;

    public static final byte[] EXISTED_ARRAY = new byte[]{EXISTED};
    public static final byte[] DELETED_ARRAY = new byte[]{DELETED};

    @Override
    public int compareTo(KeyEntry o) {
        return ByteArrayUtils.compare(key, o.key);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        return bytesList;
    }

    @Override
    public String toString() {
        return G.I.toString(toBytes());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyEntry dbKey = (KeyEntry) o;
        return Arrays.equals(key, dbKey.key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }
}
