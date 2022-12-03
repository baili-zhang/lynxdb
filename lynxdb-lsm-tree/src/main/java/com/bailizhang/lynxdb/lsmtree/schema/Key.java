package com.bailizhang.lynxdb.lsmtree.schema;

import com.bailizhang.lynxdb.core.common.WrappedBytes;

public class Key extends WrappedBytes {
    public Key(byte[] val) {
        super(val);
    }
}
