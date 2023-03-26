package com.bailizhang.lynxdb.core.common;

import java.util.List;

public interface BytesListConvertible extends BytesConvertible {

    BytesList toBytesList();

    @Override
    default byte[] toBytes() {
        return toBytesList().toBytes();
    }

    default List<byte[]> toList() {
        return toBytesList().toList();
    }
}
