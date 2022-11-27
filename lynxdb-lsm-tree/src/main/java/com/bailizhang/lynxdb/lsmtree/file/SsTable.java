package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.*;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.DELETED_ARRAY;

public class SsTable {
    private static final int BLOOM_FILTER_BEGIN = 0;

    private final BloomFilter bloomFilter;
    private final LogGroup valueLogGroup;
    private final List<DbIndex> dbIndexList;

    public SsTable(String dir, int id, int levelNo, LogGroup logGroup, Options options) {
        // 构造函数，从文件中恢复数据
        int ssTableSize = options.memTableSize()
                * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, (levelNo - LevelTree.LEVEL_BEGIN));

        String filename = NameUtils.name(id);
        FileUtils.createFileIfNotExisted(dir, filename);

        Path filePath = Path.of(dir, filename);

        FileChannel readChannel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.READ
        );

        bloomFilter = BloomFilter.from(filePath, BLOOM_FILTER_BEGIN, ssTableSize);

        valueLogGroup = logGroup;

        // 从持久化的文件中恢复数据
        int dataBegin = bloomFilter.byteCount();
        dbIndexList = DbIndex.listFrom(readChannel, dataBegin);
    }

    public static void create(Path filePath, List<DbIndex> dbIndexList) {
        int count = dbIndexList.size();

        FileUtils.createFileIfNotExisted(filePath.toFile());
        FileChannel channel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );

        BloomFilter bloomFilter = new BloomFilter(count);
        dbIndexList.forEach(dbIndex -> bloomFilter.setObj(dbIndex.key()));

        BytesList bytesList = new BytesList(false);
        bytesList.appendRawBytes(bloomFilter.data());
        dbIndexList.forEach(bytesList::append);

        List<byte[]> list = bytesList.toBytesList();

        int length = list.stream().mapToInt(bytes -> bytes.length).sum();

        MappedByteBuffer mappedBuffer = FileChannelUtils.map(
                channel,
                FileChannel.MapMode.READ_WRITE,
                BLOOM_FILTER_BEGIN,
                length
        );

        list.forEach(mappedBuffer::put);
        mappedBuffer.force();
    }

    public List<DbIndex> dbIndexList() {
        return dbIndexList;
    }

    public boolean contains(DbKey dbKey) {
        return bloomFilter.isExist(dbKey);
    }

    public byte[] find(DbKey dbKey) {
        Integer globalIndex = findValueGlobalIndex(dbKey);
        if(globalIndex == null) {
            return null;
        }

        LogEntry entry = valueLogGroup.find(globalIndex);

        if(Arrays.equals(entry.index().extraData(), DELETED_ARRAY)) {
            return null;
        }

        return entry.data();
    }

    public List<DbValue> find(byte[] key) {
        List<DbValue> values = new ArrayList<>();

        for(DbIndex dbIndex : dbIndexList) {
            DbKey dbKey = dbIndex.key();
            if(Arrays.equals(dbKey.key(), key)) {
                LogEntry logEntry = valueLogGroup.find(dbIndex.valueGlobalIndex());
                byte[] value = logEntry.data();

                values.add(new DbValue(dbKey.column(), value));
            }
        }

        return values;
    }

    public boolean delete(DbKey dbKey) {
        Integer globalIndex = findValueGlobalIndex(dbKey);
        if(globalIndex == null) {
            return false;
        }

        valueLogGroup.setExtraData(globalIndex, DELETED_ARRAY);

        return false;
    }

    private Integer findValueGlobalIndex(DbKey dbKey) {
        if(bloomFilter.isNotExist(dbKey)) {
            return null;
        }

        Optional<DbIndex> optional = dbIndexList.stream()
                .filter(dbIndex -> {
                    DbKey dbIndexKey = dbIndex.key();
                    return Arrays.equals(dbIndexKey.key(), dbKey.key())
                            && Arrays.equals(dbIndexKey.column(), dbKey.column());
                }).findFirst();

        Integer globalIndex = null;

        if(optional.isPresent()) {
            globalIndex = optional.get().valueGlobalIndex();
        }

        return globalIndex;
    }
}
