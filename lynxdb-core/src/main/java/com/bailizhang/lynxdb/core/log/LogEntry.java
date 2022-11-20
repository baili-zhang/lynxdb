package com.bailizhang.lynxdb.core.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

public record LogEntry(
        LogIndex index,
        byte[] data
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        bytesList.appendRawBytes(index.extraData());
        bytesList.appendRawLong(index.dataBegin());

        return bytesList;
    }
}
