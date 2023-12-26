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

import com.bailizhang.lynxdb.core.utils.Crc32cUtils;

import java.nio.ByteBuffer;

public record MetaHeader(
        int metaRegionLength,
        int magicNumber,
        int memTableSize,
        int maxKeyAmount,
        int keyAmount,
        int bloomFilterRegionLength,
        int firstIndexRegionLength,
        int secondIndexRegionLength,
        int dataRegionLength
) {
    public static MetaHeader from(ByteBuffer buffer) {
        Crc32cUtils.check(buffer);

        int metaRegionLength = buffer.getInt(SsTable.Default.META_REGION_LENGTH_OFFSET);
        int magicNumber = buffer.getInt(SsTable.Default.MAGIC_NUMBER_OFFSET);
        int memTableSize = buffer.getInt(SsTable.Default.MEM_TABLE_SIZE_OFFSET);
        int maxKeySize = buffer.getInt(SsTable.Default.MAX_KEY_SIZE_OFFSET);
        int keySize = buffer.getInt(SsTable.Default.KEY_SIZE_OFFSET);
        int bloomFilterRegionLength = buffer.getInt(SsTable.Default.BLOOM_FILTER_REGION_LENGTH_OFFSET);
        int firstIndexRegionLength = buffer.getInt(SsTable.Default.FIRST_INDEX_REGION_LENGTH_OFFSET);
        int secondIndexRegionLength = buffer.getInt(SsTable.Default.SECOND_INDEX_REGION_LENGTH_OFFSET);
        int dataRegionLength = buffer.getInt(SsTable.Default.DATA_REGION_LENGTH_OFFSET);

        return new MetaHeader(
                metaRegionLength,
                magicNumber,
                memTableSize,
                maxKeySize,
                keySize,
                bloomFilterRegionLength,
                firstIndexRegionLength,
                secondIndexRegionLength,
                dataRegionLength
        );
    }
}
