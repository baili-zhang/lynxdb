package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

public record DbKey(byte[] key, byte[] column) implements Comparable<DbKey>, BytesListConvertible {
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
}
