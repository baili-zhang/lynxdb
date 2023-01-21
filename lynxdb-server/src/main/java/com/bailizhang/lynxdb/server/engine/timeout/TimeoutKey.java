package com.bailizhang.lynxdb.server.engine.timeout;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;

public record TimeoutKey(
        byte[] key,
        byte[] columnFamily
) implements BytesListConvertible {
    public static TimeoutKey from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        return new TimeoutKey(key, columnFamily);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);

        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        return bytesList;
    }
}
