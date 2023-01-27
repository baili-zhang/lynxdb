package com.bailizhang.lynxdb.server.engine.message;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record MessageKey(
        byte[] key,
        byte[] columnFamily
) implements BytesListConvertible {

    public static MessageKey from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        return new MessageKey(key, columnFamily);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        return bytesList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageKey messageKey = (MessageKey) o;
        return Arrays.equals(key, messageKey.key)
                && Arrays.equals(columnFamily, messageKey.columnFamily);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(columnFamily);
        return result;
    }
}
