package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

public record DbKey(byte[] key, byte[] column, byte flag)
        implements Comparable<DbKey>, BytesListConvertible {

    public static final byte EXISTED = (byte) 0x01;
    public static final byte DELETED = (byte) 0x02;

    public static final byte[] EXISTED_ARRAY = new byte[]{EXISTED};
    public static final byte[] DELETED_ARRAY = new byte[]{DELETED};

    // TODO: 补充 CRC 校验

    @Override
    public int compareTo(DbKey o) {
        if (!Arrays.equals(key, o.key)) {
            return ByteArrayUtils.compare(key, o.key);
        }

        return ByteArrayUtils.compare(column, o.column);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
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
        DbKey dbKey = (DbKey) o;
        return Arrays.equals(key, dbKey.key) && Arrays.equals(column, dbKey.column);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(column);
        return result;
    }
}
