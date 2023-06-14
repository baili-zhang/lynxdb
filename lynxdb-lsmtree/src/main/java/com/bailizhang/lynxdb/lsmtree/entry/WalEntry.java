package com.bailizhang.lynxdb.lsmtree.entry;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record WalEntry(
        byte flag,
        byte[] key,
        byte[] value,
        int valueGlobalIndex
) implements BytesListConvertible {

    public static WalEntry from(byte flag, byte[] key, byte[] value, int valueGlobalIndex) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(key);
        crc32C.update(value);
        crc32C.update(valueGlobalIndex);

        return new WalEntry(flag, key, value, valueGlobalIndex);
    }

    public static WalEntry from(ByteBuffer buffer) {
        byte flag = buffer.get();
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();

        if (flag != KeyEntry.EXISTED && flag != KeyEntry.DELETED) {
            throw new RuntimeException();
        }

        return new WalEntry(flag, key, value, valueGlobalIndex);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(flag);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(value);
        bytesList.appendRawInt(valueGlobalIndex);

        return bytesList;
    }
}
