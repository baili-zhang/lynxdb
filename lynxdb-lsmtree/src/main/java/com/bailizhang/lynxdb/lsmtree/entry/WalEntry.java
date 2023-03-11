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
        int valueGlobalIndex,
        long crc32c
) implements BytesListConvertible {

    public static WalEntry from(byte flag, byte[] key, byte[] value, int valueGlobalIndex) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(key);
        crc32C.update(value);
        crc32C.update(valueGlobalIndex);

        long crc32c = crc32C.getValue();

        return new WalEntry(flag, key, value, valueGlobalIndex, crc32c);
    }

    public static WalEntry from(ByteBuffer buffer) {
        byte flag = buffer.get();
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(key);
        crc32C.update(value);
        crc32C.update(valueGlobalIndex);

        if(crc32c != crc32C.getValue()) {
            throw new RuntimeException("Data Error");
        }

        if (flag != KeyEntry.EXISTED && flag != KeyEntry.DELETED) {
            throw new RuntimeException();
        }

        return new WalEntry(flag, key, value, valueGlobalIndex, crc32c);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(flag);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(value);
        bytesList.appendRawInt(valueGlobalIndex);
        bytesList.appendRawLong(crc32c);

        return bytesList;
    }
}
