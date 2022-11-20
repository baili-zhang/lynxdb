package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

public class DbKey implements Comparable<DbKey>, BytesListConvertible {
    private final byte[] key;
    private final byte[] column;
    private final long timestamp;

    // TODO: 补充 CRC 校验

    public DbKey(byte[] key, byte[] column, long timestamp) {
        this.key = key;
        this.column = column;
        this.timestamp = timestamp;
    }

    public byte[] key() {
        return key;
    }

    public byte[] column() {
        return column;
    }

    public long timestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(DbKey o) {
        if(!Arrays.equals(key, o.key)) {
            return ByteArrayUtils.compare(key, o.key);
        }

        if(!Arrays.equals(column, o.column)) {
            return ByteArrayUtils.compare(column, o.column);
        }

        return Long.compare(timestamp, o.timestamp);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
        bytesList.appendRawLong(timestamp);
        return bytesList;
    }

    @Override
    public String toString() {
        return G.I.toString(toBytes());
    }
}
