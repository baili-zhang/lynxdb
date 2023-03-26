package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

public record LogDataEntry(
        byte[] data,
        long crc32c
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawBytes(data);
        bytesList.appendRawLong(crc32c);
        return bytesList;
    }
}
