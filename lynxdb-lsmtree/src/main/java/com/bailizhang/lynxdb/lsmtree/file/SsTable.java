package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbIndex;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.BYTE_LENGTH;
import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.lsmtree.common.DbKey.DELETED;
import static com.bailizhang.lynxdb.lsmtree.common.DbKey.EXISTED;

/**
 * TODO: 支持二分查找，内存映射
 */
public class SsTable {
    private static final int SIZE_BEGIN = 0;
    private static final int BLOOM_FILTER_BEGIN = INT_LENGTH;
    private static final int INDEX_ENTRY_LENGTH = BYTE_LENGTH + 2 * INT_LENGTH;

    private final int ssTableSize;

    private final MappedBuffer sizeBuffer;

    private final BloomFilter bloomFilter;

    private final MappedBuffer indexBuffer;
    private final MappedBuffer keyBuffer;

    private final LogGroup valueLogGroup;

    public SsTable(Path filePath, int levelNo, LogGroup logGroup, Options options) {
        sizeBuffer = new MappedBuffer(filePath, SIZE_BEGIN, INT_LENGTH);
        int size = size();

        ssTableSize = SsTable.ssTableSize(levelNo, options);
        bloomFilter = BloomFilter.from(
                filePath,
                BLOOM_FILTER_BEGIN,
                ssTableSize
        );

        int indexBegin = bloomFilter.length() + BLOOM_FILTER_BEGIN;
        int indexLength = INDEX_ENTRY_LENGTH * ssTableSize;
        indexBuffer = new MappedBuffer(filePath, indexBegin, indexLength);

        Index lastIndex = findIndex(size - 1);

        int keyBegin = indexBegin + indexLength;
        int keyLength = lastIndex.begin() + lastIndex.length();
        keyBuffer = new MappedBuffer(filePath, keyBegin, keyLength);

        valueLogGroup = logGroup;
    }

    public SsTable(int ssTableSize, MappedBuffer sizeBuffer, BloomFilter bloomFilter,
            MappedBuffer indexBuffer, MappedBuffer keyBuffer, LogGroup valueLogGroup) {
        this.ssTableSize = ssTableSize;
        this.sizeBuffer = sizeBuffer;
        this.bloomFilter = bloomFilter;
        this.indexBuffer = indexBuffer;
        this.keyBuffer = keyBuffer;
        this.valueLogGroup = valueLogGroup;
    }

    public static SsTable create(Path filePath, int levelNo, Options options,
                                 List<DbIndex> dbIndexList, LogGroup valueLogGroup) {
        FileUtils.createFileIfNotExisted(filePath.toFile());

        MappedBuffer sizeBuffer = new MappedBuffer(filePath, SIZE_BEGIN, INT_LENGTH);
        MappedByteBuffer sizeMappedBuffer = sizeBuffer.getBuffer();
        int size = options.memTableSize() * (int)Math.pow(10, levelNo - 1);

        if(dbIndexList.size() > size) {
            throw new RuntimeException();
        }

        sizeMappedBuffer.putInt(SIZE_BEGIN, size);

        int ssTableSize = SsTable.ssTableSize(levelNo, options);
        BloomFilter bloomFilter = BloomFilter.from(
                filePath,
                BLOOM_FILTER_BEGIN,
                ssTableSize
        );
        dbIndexList.forEach(dbIndex -> bloomFilter.setObj(dbIndex.key().toBytes()));

        int indexBegin = bloomFilter.length() + BLOOM_FILTER_BEGIN;
        int indexLength = INDEX_ENTRY_LENGTH * ssTableSize;
        MappedBuffer indexBuffer = new MappedBuffer(filePath, indexBegin, indexLength);

        List<Key> keys = dbIndexList.stream()
                .map(dbIndex -> {
                    DbKey dbKey = dbIndex.key();
                    return new Key(dbKey.flag(), dbIndex.toBytes());
                })
                .toList();
        int keyBegin = indexBegin + indexLength;
        int keyLength = keys.stream().mapToInt(key -> key.data().length).sum();
        MappedBuffer keyBuffer = new MappedBuffer(filePath, keyBegin, keyLength);

        AtomicInteger entryBegin = new AtomicInteger();
        keys.forEach(key -> {
            byte flag = key.flag();
            byte[] data = key.data();

            MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
            indexMappedBuffer.put(flag);
            indexMappedBuffer.putInt(entryBegin.getAndAdd(data.length));
            indexMappedBuffer.putInt(data.length);

            MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
            keyMappedBuffer.put(data);
        });

        sizeBuffer.force();
        bloomFilter.force();
        indexBuffer.force();
        keyBuffer.force();

        return new SsTable(ssTableSize, sizeBuffer, bloomFilter, indexBuffer, keyBuffer, valueLogGroup);
    }

    public void all(HashSet<DbIndex> set) {
        MappedByteBuffer indexMapperBuffer = indexBuffer.getBuffer();
        indexMapperBuffer.rewind();

        MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
        keyMappedBuffer.rewind();

        while(BufferUtils.isNotOver(keyMappedBuffer)) {
            Index index = Index.from(indexMapperBuffer);
            DbIndex dbIndex = DbIndex.from(index.flag(), keyMappedBuffer);

            if(set.contains(dbIndex)) {
                int position = indexMapperBuffer.position();
                int flagPosition = position - INDEX_ENTRY_LENGTH;
                indexMapperBuffer.put(flagPosition, DELETED);
                continue;
            }

            set.add(dbIndex);
        }
    }

    public boolean contains(DbKey dbKey) {
        return bloomFilter.isExist(dbKey.toBytes());
    }

    public byte[] find(DbKey dbKey) throws DeletedException {
        int idx = findIndex(dbKey);
        if(idx >= size()) {
            return null;
        }

        DbIndex dbIndex = findDbIndex(idx);
        DbKey existed = dbIndex.key();
        if(existed.compareTo(dbKey) != 0) {
            return null;
        }

        if(existed.flag() == DELETED) {
            throw new DeletedException();
        }

        int globalIndex = dbIndex.valueGlobalIndex();
        LogEntry entry = valueLogGroup.find(globalIndex);

        return entry.data();
    }

    public void find(byte[] key, HashSet<DbValue> dbValues) {
        int idx = findIndex(new DbKey(key, BufferUtils.EMPTY_BYTES, EXISTED));

        while (idx < size() - 1) {
            DbIndex dbIndex = findDbIndex(idx);
            DbKey dbKey = dbIndex.key();

            if(!Arrays.equals(key, dbKey.key())) {
                break;
            }

            byte[] column = dbKey.column();
            int globalIndex = dbIndex.valueGlobalIndex();
            LogEntry entry = valueLogGroup.find(globalIndex);

            DbValue dbValue = new DbValue(column, entry.data());

            idx ++;

            // 只有不存在的时候才 add
            if(dbValues.contains(dbValue)) {
                continue;
            }

            dbValues.add(dbValue);
        }
    }

    private static int ssTableSize(int levelNo, Options options) {
        return options.memTableSize()
                * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, (levelNo - LevelTree.LEVEL_BEGIN));
    }

    // 找第一个大于等于 dbKey 的下标
    private int findIndex(DbKey dbKey) {
        int begin = 0, end = size() - 1, mid, idx = size();

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            DbIndex midDbIndex = findDbIndex(mid);
            DbKey midDbKey = midDbIndex.key();

            if(dbKey.compareTo(midDbKey) <= 0) {
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

    private Index findIndex(int idx) {
        MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
        indexMappedBuffer.position(idx * INDEX_ENTRY_LENGTH);

        return Index.from(indexMappedBuffer);
    }

    private DbIndex findDbIndex(int idx) {
        Index index = findIndex(idx);

        MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
        keyMappedBuffer.position(index.begin());

        return DbIndex.from(index.flag(), keyMappedBuffer);
    }

    private record Index (byte flag, int begin, int length) {
        static Index from(ByteBuffer buffer) {
            byte flag = buffer.get();
            int begin = buffer.getInt();
            int length = buffer.getInt();
            return new Index(flag, begin, length);
        }
    }

    private record Key (byte flag, byte[] data) {

    }
}