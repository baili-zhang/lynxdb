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

package com.bailizhang.lynxdb.table.entry;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.utils.BufferUtils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

public record WalEntry(
        byte flag,
        byte[] key,
        byte[] value,
        int valueGlobalIndex,
        long timeout
) {

    public static WalEntry from(
            byte flag,
            byte[] key,
            byte[] value,
            int valueGlobalIndex,
            long timeout
    ) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(key);
        crc32C.update(value);
        crc32C.update(valueGlobalIndex);
        crc32C.update(BufferUtils.toBytes(timeout));

        return new WalEntry(
                flag,
                key,
                value,
                valueGlobalIndex,
                timeout
        );
    }

    public static WalEntry from(ByteBuffer buffer) {
        byte flag = buffer.get();
        byte[] key = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);
        int valueGlobalIndex = buffer.getInt();
        long timeout = buffer.getLong();

        if (flag != Flags.EXISTED && flag != Flags.DELETED) {
            throw new RuntimeException();
        }

        return new WalEntry(
                flag,
                key,
                value,
                valueGlobalIndex,
                timeout
        );
    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(flag);
        dataBlocks.appendVarBytes(key);
        dataBlocks.appendVarBytes(value);
        dataBlocks.appendRawInt(valueGlobalIndex);
        dataBlocks.appendRawLong(timeout);

        return dataBlocks.toBuffers();
    }
}
