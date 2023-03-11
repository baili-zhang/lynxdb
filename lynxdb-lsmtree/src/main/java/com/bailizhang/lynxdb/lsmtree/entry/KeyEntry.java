package com.bailizhang.lynxdb.lsmtree.entry;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;

/**
 * 持久化到 SSTable 时用到的对象
 *
 * @param flag flag
 * @param key key
 * @param valueGlobalIndex value global index
 * @param crc32c crc32c
 */
public record KeyEntry(
        byte flag, // 持久化在 index 中，不需要 crc，也不用转成 bytes
        byte[] key,
        byte[] value, // memTable 需要这个字段，不需要 crc，也不用转成 bytes
        int valueGlobalIndex,
        long crc32c
) implements Comparable<KeyEntry>, BytesListConvertible {

    public static final byte EXISTED = (byte) 0x01;
    public static final byte DELETED = (byte) 0x02;

    public static final byte[] EXISTED_ARRAY = new byte[]{EXISTED};
    public static final byte[] DELETED_ARRAY = new byte[]{DELETED};

    public static KeyEntry from(byte flag, byte[] key, byte[] value, int valueGlobalIndex) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(key);
        crc32C.update(valueGlobalIndex);

        long crc32c = crc32C.getValue();

        return new KeyEntry(flag, key, value, valueGlobalIndex, crc32c);
    }

    public static KeyEntry from(WalEntry walEntry) {
        byte[] key = walEntry.key();
        int valueGlobalIndex = walEntry.valueGlobalIndex();

        CRC32C crc32C = new CRC32C();
        crc32C.update(key);
        crc32C.update(valueGlobalIndex);

        long crc32c = crc32C.getValue();

        return new KeyEntry(
                walEntry.flag(),
                key,
                walEntry.value(),
                valueGlobalIndex,
                crc32c
        );
    }

    public static KeyEntry from(byte flag, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] key = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(key);
        crc32C.update(valueGlobalIndex);

        if(crc32c != crc32C.getValue()) {
            throw new RuntimeException("Data Error");
        }

        return new KeyEntry(flag, key, null, valueGlobalIndex, crc32c);
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList(false);
        bytesList.appendVarBytes(key);
        bytesList.appendRawInt(valueGlobalIndex);
        bytesList.appendRawLong(crc32c);

        return bytesList;
    }

    public int length() {
        return INT_LENGTH + key.length + INT_LENGTH + LONG_LENGTH;
    }

    @Override
    public int compareTo(KeyEntry o) {
        return Arrays.compare(key, o.key);
    }

    @Override
    public String toString() {
        return "do later";
    }
}
