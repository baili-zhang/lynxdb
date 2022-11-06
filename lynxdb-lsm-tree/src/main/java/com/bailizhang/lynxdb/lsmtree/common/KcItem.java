package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Arrays;

/**
 * KC: (key, column) pair
 */
public class KcItem implements Comparable<KcItem>, BytesConvertible {
    private final byte[] key;
    private final byte[] column;

    public KcItem(byte[] keyBytes, byte[] columnBytes) {
        key = keyBytes;
        column = columnBytes;
    }

    public byte[] key() {
        return key;
    }

    public byte[] column() {
        return column;
    }

    @Override
    public int compareTo(KcItem o) {
        if(Arrays.equals(key, o.key)) {
            return ByteArrayUtils.compare(column, o.column);
        }
        return ByteArrayUtils.compare(key, o.key);
    }

    @Override
    public byte[] toBytes() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
        return bytesList.toBytes();
    }
}
