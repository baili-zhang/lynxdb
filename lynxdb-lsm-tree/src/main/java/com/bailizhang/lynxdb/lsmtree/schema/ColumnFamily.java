package com.bailizhang.lynxdb.lsmtree.schema;

import com.bailizhang.lynxdb.core.common.WrappedBytes;

public class ColumnFamily extends WrappedBytes {
    public ColumnFamily(byte[] val) {
        super(val);
    }
}
