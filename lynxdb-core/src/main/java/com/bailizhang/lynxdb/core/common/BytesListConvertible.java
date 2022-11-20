package com.bailizhang.lynxdb.core.common;

public interface BytesListConvertible extends BytesConvertible {
    BytesList toBytesList();

    @Override
    default byte[] toBytes() {
        return toBytesList().toBytes();
    }
}
