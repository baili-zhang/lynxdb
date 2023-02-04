package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

public record DbEntry(DbKey key, byte[] value) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.append(key.toBytesList());
        bytesList.appendVarBytes(value);
        return bytesList;
    }
}
