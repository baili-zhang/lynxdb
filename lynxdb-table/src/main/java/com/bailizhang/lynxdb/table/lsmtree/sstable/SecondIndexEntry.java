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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

public record SecondIndexEntry(
        byte flag, // 是否删除
        int begin, // 顺序查找不需要，二分查找需要这个字段
        int length,
        long crc32c
) {

    public static SecondIndexEntry from(byte flag, int begin, int length) {
        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(begin);
        crc32C.update(length);

        long crc32c = crc32C.getValue();

        return new SecondIndexEntry(flag, begin, length, crc32c);
    }

    public static SecondIndexEntry from(ByteBuffer buffer) {
        byte flag = buffer.get();
        int begin = buffer.getInt();
        int length = buffer.getInt();
        long crc32c = buffer.getLong();

        CRC32C crc32C = new CRC32C();
        crc32C.update(new byte[]{flag});
        crc32C.update(begin);
        crc32C.update(length);

        if(crc32c != crc32C.getValue()) {
            throw new RuntimeException("Data Error");
        }

        return new SecondIndexEntry(flag, begin, length, crc32c);
    }

    public static void writeToBuffer(List<SecondIndexEntry> entries, ByteBuffer buffer) {

    }

    public ByteBuffer[] toBuffers() {
        DataBlocks dataBlocks = new DataBlocks(false);
        dataBlocks.appendRawByte(flag);
        dataBlocks.appendRawInt(begin);
        dataBlocks.appendRawInt(length);
        dataBlocks.appendRawLong(crc32c);

        return dataBlocks.toBuffers();
    }
}
