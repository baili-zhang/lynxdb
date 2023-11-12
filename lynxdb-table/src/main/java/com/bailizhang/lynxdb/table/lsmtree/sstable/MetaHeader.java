package com.bailizhang.lynxdb.table.lsmtree.sstable;

import com.bailizhang.lynxdb.table.utils.Crc32cUtils;

import java.nio.ByteBuffer;

public record MetaHeader(
        int metaRegionLength,
        int magicNumber,
        int memTableSize,
        int maxKeySize,
        int keySize,
        int bloomFilterRegionLength,
        int firstIndexRegionLength,
        int secondIndexRegionLength,
        int dataRegionLength,
        long crc32c
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
        long crc32c = buffer.getLong(SsTable.Default.CRC32C_OFFSET);

        return new MetaHeader(
                metaRegionLength,
                magicNumber,
                memTableSize,
                maxKeySize,
                keySize,
                bloomFilterRegionLength,
                firstIndexRegionLength,
                secondIndexRegionLength,
                dataRegionLength,
                crc32c
        );
    }
}
