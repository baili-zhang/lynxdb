package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbIndex;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.DELETED_ARRAY;

/**
 * TODO: 支持二分查找，内存映射
 */
public class SsTable {
    private static final int SIZE_BEGIN = 0;
    private static final int BLOOM_FILTER_BEGIN = INT_LENGTH;
    private static final int INDEX_ENTRY_LENGTH = 2 * INT_LENGTH;

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

        MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
        indexMappedBuffer.position(indexBegin + (ssTableSize - 1) * INDEX_ENTRY_LENGTH);
        Index lastIndex = findIndex(size - 1);

        int keyBegin = indexBegin + indexLength;
        int keyLength = lastIndex.begin() + lastIndex.length() - keyBegin;
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

    // TODO: 性能浪费在 List<DbIndex> 拷贝上了
    public static SsTable create(Path filePath, int levelNo, Options options,
                                 List<DbIndex> dbIndexList, LogGroup valueLogGroup) {
        FileUtils.createFileIfNotExisted(filePath.toFile());

        MappedBuffer sizeBuffer = new MappedBuffer(filePath, SIZE_BEGIN, INT_LENGTH);
        MappedByteBuffer sizeMappedBuffer = sizeBuffer.getBuffer();
        sizeMappedBuffer.putInt(SIZE_BEGIN, dbIndexList.size());

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

        List<byte[]> keys = dbIndexList.stream().map(DbIndex::toBytes).toList();
        int keyBegin = indexBegin + indexLength;
        int keyLength = keys.stream().mapToInt(key -> key.length).sum();
        MappedBuffer keyBuffer = new MappedBuffer(filePath, keyBegin, keyLength);

        AtomicInteger entryBegin = new AtomicInteger();
        keys.forEach(key -> {
            MappedByteBuffer indexMappedBuffer = indexBuffer.getBuffer();
            indexMappedBuffer.putInt(entryBegin.getAndAdd(key.length));
            indexMappedBuffer.putInt(key.length);

            MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
            keyMappedBuffer.put(key);
        });

        sizeBuffer.force();
        bloomFilter.force();
        indexBuffer.force();
        keyBuffer.force();

        return new SsTable(ssTableSize, sizeBuffer, bloomFilter, indexBuffer, keyBuffer, valueLogGroup);
    }

    public List<DbIndex> dbIndexList() {
        List<DbIndex> dbIndexList = new ArrayList<>();

        MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
        keyMappedBuffer.rewind();

        while(BufferUtils.isNotOver(keyMappedBuffer)) {
            dbIndexList.add(DbIndex.from(keyMappedBuffer));
        }

        return dbIndexList;
    }

    public boolean contains(DbKey dbKey) {
        return bloomFilter.isExist(dbKey.toBytes());
    }

    public byte[] find(DbKey dbKey) {
        int idx = findIndex(dbKey);
        if(idx >= size()) {
            return null;
        }

        DbIndex dbIndex = findDbIndex(idx);
        if(dbIndex.key().compareTo(dbKey) != 0) {
            return null;
        }

        int globalIndex = dbIndex.valueGlobalIndex();
        LogEntry entry = valueLogGroup.find(globalIndex);

        if(Arrays.equals(entry.index().extraData(), DELETED_ARRAY)) {
            return null;
        }

        return entry.data();
    }

    public List<DbValue> find(byte[] key) {
        List<DbValue> values = new ArrayList<>();
        int idx = findIndex(new DbKey(key, BufferUtils.EMPTY_BYTES));

        while (idx < size() - 1) {
            DbIndex dbIndex = findDbIndex(idx);
            DbKey dbKey = dbIndex.key();

            if(!Arrays.equals(key, dbKey.key())) {
                break;
            }

            byte[] column = dbKey.column();
            int globalIndex = dbIndex.valueGlobalIndex();
            LogEntry entry = valueLogGroup.find(globalIndex);

            values.add(new DbValue(column, entry.data()));

            idx ++;
        }

        return values;
    }

    public boolean delete(DbKey dbKey) {
        int idx = findIndex(dbKey);
        if(idx >= size()) {
            return false;
        }

        DbIndex dbIndex = findDbIndex(idx);
        if(dbIndex.key().compareTo(dbKey) != 0) {
            return false;
        }

        int globalIndex = dbIndex.valueGlobalIndex();
        valueLogGroup.setExtraData(globalIndex, DELETED_ARRAY);

        return true;
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

        int begin = indexMappedBuffer.getInt();
        int length = indexMappedBuffer.getInt();

        return new Index(begin, length);
    }

    private DbIndex findDbIndex(int idx) {
        Index index = findIndex(idx);

        MappedByteBuffer keyMappedBuffer = keyBuffer.getBuffer();
        keyMappedBuffer.position(index.begin());

        return DbIndex.from(keyMappedBuffer);
    }

    private record Index (int begin, int length) {

    }
}
