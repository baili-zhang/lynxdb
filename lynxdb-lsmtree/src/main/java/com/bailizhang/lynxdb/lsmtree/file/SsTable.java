package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.entry.IndexEntry;
import com.bailizhang.lynxdb.lsmtree.entry.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

/**
 * TODO: 元数据，布隆过滤器，索引，entry 都需要添加 crc 检验字段
 */
public class SsTable {
    private static final int SIZE_BEGIN = 0;
    private static final int BLOOM_FILTER_BEGIN = INT_LENGTH;
    private static final int INDEX_ENTRY_LENGTH = BYTE_LENGTH + 2 * INT_LENGTH + LONG_LENGTH;

    private final int ssTableSize;

    private final MappedBuffer sizeBuffer;


    private final BloomFilter bloomFilter;

    private final MappedBuffer indexBuffer;
    private final MappedBuffer keyBuffer;

    private final LogGroup valueLogGroup;

    public SsTable(
            Path filePath,
            int levelNo,
            LogGroup logGroup,
            LsmTreeOptions options
    ) {
        sizeBuffer = new MappedBuffer(filePath, SIZE_BEGIN, INT_LENGTH);
        int size = size();

        ssTableSize = SsTable.ssTableSize(levelNo, options);
        bloomFilter = BloomFilter.from(
                filePath,
                BLOOM_FILTER_BEGIN,
                ssTableSize
        );

        int indexBegin = BLOOM_FILTER_BEGIN + bloomFilter.length();
        int indexLength = INDEX_ENTRY_LENGTH * ssTableSize;
        indexBuffer = new MappedBuffer(filePath, indexBegin, indexLength);

        IndexEntry lastIndex = findIndexEntry(size - 1);

        int keyBegin = indexBegin + indexLength;
        int keyLength = lastIndex.begin() + lastIndex.length();
        keyBuffer = new MappedBuffer(filePath, keyBegin, keyLength);

        valueLogGroup = logGroup;
    }

    public SsTable(
            int ssTableSize,
            MappedBuffer sizeBuffer,
            BloomFilter bloomFilter,
            MappedBuffer indexBuffer,
            MappedBuffer keyBuffer,
            LogGroup valueLogGroup
    ) {
        this.ssTableSize = ssTableSize;
        this.sizeBuffer = sizeBuffer;
        this.bloomFilter = bloomFilter;
        this.indexBuffer = indexBuffer;
        this.keyBuffer = keyBuffer;
        this.valueLogGroup = valueLogGroup;
    }

    public static SsTable create(
            Path filePath,
            int levelNo,
            LsmTreeOptions options,
            List<KeyEntry> keyEntries,
            LogGroup valueLogGroup
    ) {
        FileUtils.createFileIfNotExisted(filePath.toFile());

        MappedBuffer sizeBuffer = new MappedBuffer(filePath, SIZE_BEGIN, INT_LENGTH);
        MappedByteBuffer sizeMappedBuffer = sizeBuffer.getBuffer();
        int ssTableSize = SsTable.ssTableSize(levelNo, options);

        if(keyEntries.size() > ssTableSize) {
            throw new RuntimeException();
        }

        sizeMappedBuffer.putInt(SIZE_BEGIN, ssTableSize);

        BloomFilter bloomFilter = BloomFilter.from(
                filePath,
                BLOOM_FILTER_BEGIN,
                ssTableSize
        );
        keyEntries.forEach(keyEntry -> bloomFilter.setObj(keyEntry.key()));

        int indexBegin = BLOOM_FILTER_BEGIN + bloomFilter.length();
        int indexLength = INDEX_ENTRY_LENGTH * ssTableSize;
        MappedBuffer indexBuffer = new MappedBuffer(filePath, indexBegin, indexLength);

        int keyBegin = indexBegin + indexLength;
        int keyLength = keyEntries.stream().mapToInt(KeyEntry::length).sum();
        MappedBuffer keyBuffer = new MappedBuffer(filePath, keyBegin, keyLength);

        AtomicInteger entryBegin = new AtomicInteger();
        keyEntries.forEach(entry -> {
            byte[] data = entry.toBytes();

            byte flag = entry.flag();
            int begin = entryBegin.getAndAdd(data.length);
            int length = entry.length();
            IndexEntry indexEntry = IndexEntry.from(flag, begin, length);

            byte[] indexData = indexEntry.toBytes();

            MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
            indexMappedBuffer.put(indexData);

            MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
            keyMappedBuffer.put(data);
        });

        sizeBuffer.force();
        bloomFilter.force();
        indexBuffer.force();
        keyBuffer.force();

        return new SsTable(
                ssTableSize,
                sizeBuffer,
                bloomFilter,
                indexBuffer,
                keyBuffer,
                valueLogGroup
        );
    }

    public void all(HashSet<KeyEntry> set) {
        MappedByteBuffer indexMapperBuffer = indexBuffer.getBuffer();
        indexMapperBuffer.rewind();

        MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
        keyMappedBuffer.rewind();

        // MemTable 可能不是最大容量
        while(BufferUtils.isNotOver(keyMappedBuffer)) {
            IndexEntry index = IndexEntry.from(indexMapperBuffer);
            int length = index.length();

            byte[] data = new byte[length];
            keyMappedBuffer.get(data);

            KeyEntry entry = KeyEntry.from(index.flag(), data);
            set.add(entry);
        }
    }

    public boolean contains(byte[] key) {
        return bloomFilter.isExist(key);
    }

    public byte[] find(byte[] key) throws DeletedException {
        int idx = findIdx(key);
        if(idx >= size()) {
            return null;
        }

        IndexEntry indexEntry = findIndexEntry(idx);

        if(indexEntry.flag() == KeyEntry.DELETED) {
            throw new DeletedException();
        }

        KeyEntry keyEntry = findKeyEntry(indexEntry);
        int globalIndex = keyEntry.valueGlobalIndex();
        LogEntry entry = valueLogGroup.find(globalIndex);

        return entry.data();
    }

    public boolean existKey(byte[] key) throws DeletedException {
        if(bloomFilter.isNotExist(key)) {
            return false;
        }

        int idx = findIdx(key);

        if(idx >= size()) {
            return false;
        }

        IndexEntry indexEntry = findIndexEntry(idx);
        KeyEntry keyEntry = findKeyEntry(indexEntry);

        if(Arrays.equals(key, keyEntry.key())) {
            if(indexEntry.flag() == KeyEntry.EXISTED) {
                return true;
            }

            throw new DeletedException();
        }

        return false;
    }


    private static int ssTableSize(int levelNo, LsmTreeOptions options) {
        return options.memTableSize()
                * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, (levelNo - LevelTree.LEVEL_BEGIN));
    }

    // 找第一个大于等于 dbKey 的下标
    private int findIdx(byte[] key) {
        int begin = 0, end = size() - 1, mid, idx = size();

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            IndexEntry midIndexEntry = findIndexEntry(mid);
            KeyEntry midKeyEntry = findKeyEntry(midIndexEntry);

            if(Arrays.compare(key, midKeyEntry.key()) <= 0) {
                idx = mid;
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        return idx;
    }

    private int size() {
        MappedByteBuffer sizeMapperBuffer = sizeBuffer.getBuffer();
        return sizeMapperBuffer.getInt(SIZE_BEGIN);
    }

    private IndexEntry findIndexEntry(int idx) {
        MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
        indexMappedBuffer.position(idx * INDEX_ENTRY_LENGTH);

        return IndexEntry.from(indexMappedBuffer);
    }

    private KeyEntry findKeyEntry(IndexEntry indexEntry) {
        ByteBuffer buffer = keyBuffer.getBuffer();
        buffer.position(indexEntry.begin());

        byte[] keyData = new byte[indexEntry.length()];
        buffer.get(keyData);

        return KeyEntry.from(indexEntry.flag(), keyData);
    }
}
