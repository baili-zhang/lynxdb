package com.bailizhang.lynxdb.ldtp.message;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public record MessageKey(
        byte[] key,
        String columnFamily
) implements BytesListConvertible {

    public static MessageKey from(ByteBuffer buffer) {
        byte[] key = BufferUtils.getBytes(buffer);
        String columnFamily = BufferUtils.getString(buffer);

        return new MessageKey(key, columnFamily);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);

        return bytesList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MessageKey that = (MessageKey) o;
        return Arrays.equals(key, that.key)
                && Objects.equals(columnFamily, that.columnFamily);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(columnFamily);
        result = 31 * result + Arrays.hashCode(key);
        return result;
    }
}
