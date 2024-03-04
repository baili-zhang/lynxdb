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

import com.bailizhang.lynxdb.core.common.FileType;
import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.LONG_LENGTH;
import static com.bailizhang.lynxdb.table.lsmtree.sstable.SsTable.META_HEADER_LENGTH;

public class MetaRegion {
    public static void writeToBuffer(
            MetaHeader metaHeader,
            byte[] beginKey,
            byte[] endKey,
            ByteBuffer buffer
    ) {
        buffer.putInt(metaHeader.metaRegionLength());
        buffer.putInt(FileType.SSTABLE_FILE.magicNumber());
        buffer.putInt(metaHeader.memTableSize());
        buffer.putInt(metaHeader.maxKeyAmount());
        buffer.putInt(metaHeader.keyAmount());
        buffer.putInt(metaHeader.bloomFilterRegionLength());
        buffer.putInt(metaHeader.firstIndexRegionLength());
        buffer.putInt(metaHeader.secondIndexRegionLength());
        buffer.putInt(metaHeader.dataRegionLength());
        Crc32cUtils.update(buffer, 0, SsTable.Default.CRC32C_OFFSET);
        buffer.putInt(beginKey.length);
        buffer.put(beginKey);
        buffer.putInt(endKey.length);
        buffer.put(endKey);
        Crc32cUtils.update(buffer, META_HEADER_LENGTH, buffer.capacity() - LONG_LENGTH);
    }
}
