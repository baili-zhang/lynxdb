package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

public class DbKey implements Comparable<DbKey>, BytesListConvertible {
    private final byte[] key;
    private final byte[] column;

    // TODO: 补充 CRC 校验

    public DbKey(byte[] key, byte[] column) {
        this.key = key;
        this.column = column;
    }

    public byte[] key() {
        return key;
    }

    public byte[] column() {
        return column;
    }

    @Override
    public int compareTo(DbKey o) {
        if(!Arrays.equals(key, o.key)) {
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
}
