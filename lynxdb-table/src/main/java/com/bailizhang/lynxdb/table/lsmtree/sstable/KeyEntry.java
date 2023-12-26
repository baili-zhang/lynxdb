/*
 * Copyright 2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.table.lsmtree.sstable;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.table.entry.WalEntry;

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
 * @param timeout timeout
 * @param crc32c crc32c
 */
public record KeyEntry(
        byte flag, // 持久化在 index 中，不需要 crc，也不用转成 bytes
        byte[] key,
        byte[] value, // memTable 需要这个字段，不需要 crc，也不用转成 bytes
        int valueGlobalIndex,
        long timeout,
        long crc32c
) implements Comparable<KeyEntry> {
    public static KeyEntry from(
            byte flag,
            byte[] key,
            byte[] value,
            int valueGlobalIndex,
            long timeout
    ) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(key);
        crc32C.update(valueGlobalIndex);
        crc32C.update(BufferUtils.toBytes(timeout));

        long crc32c = crc32C.getValue();

        return new KeyEntry(
                flag,
                key,
                value,
                valueGlobalIndex,
                timeout,
                crc32c
        );
    }

    public static KeyEntry from(WalEntry walEntry) {
        byte[] key = walEntry.key();
        int valueGlobalIndex = walEntry.valueGlobalIndex();
        long timeout = walEntry.timeout();

        return from(
                walEntry.flag(),
                key,
                walEntry.value(),
                valueGlobalIndex,
                timeout
        );
    }

    public static KeyEntry from(byte flag, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte[] key = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();
        long timeout = buffer.getLong();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(key);
        crc32C.update(valueGlobalIndex);
        crc32C.update(BufferUtils.toBytes(timeout));

        if(crc32c != crc32C.getValue()) {
            throw new RuntimeException("Data Error");
        }

        return new KeyEntry(
                flag,
                key,
                null,
                valueGlobalIndex,
                timeout,
                crc32c
        );
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendVarBytes(key);
        dataBlocks.appendRawInt(valueGlobalIndex);
        dataBlocks.appendRawLong(timeout);
        dataBlocks.appendRawLong(crc32c);

        return dataBlocks.toBuffers();
    }

    public boolean isTimeout() {
        return timeout > 0 && timeout < System.currentTimeMillis();
    }

    public int length() {
        return INT_LENGTH + key.length + INT_LENGTH + LONG_LENGTH + LONG_LENGTH;
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
