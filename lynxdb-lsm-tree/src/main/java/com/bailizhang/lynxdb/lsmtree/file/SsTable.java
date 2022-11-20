package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.*;
import com.bailizhang.lynxdb.lsmtree.memory.VersionalValue;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.DELETED_ARRAY;
import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.EXISTED_ARRAY;

public class SsTable {
    private final FileChannel indexChannel;
    private final BloomFilter bloomFilter;

    private final LogGroup valueLogGroup;

    private final List<DbIndex> dbIndexList;

    private DbKey minDbKey;
    private DbKey maxDbKey;

    public SsTable(String dir, int id, int levelNo, LogGroup logGroup, Options options) {
        int ssTableSize = options.memTableSize() * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, levelNo);

        String filename = NameUtils.name(id);
        FileUtils.createFileIfNotExisted(dir, filename);

        indexChannel = FileChannelUtils.open(
                Path.of(dir, filename),
                StandardOpenOption.APPEND,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );

        bloomFilter = new BloomFilter(indexChannel, ssTableSize);

        valueLogGroup = logGroup;
        dbIndexList = new ArrayList<>(ssTableSize);

        // 从持久化的文件中恢复数据
        int dataBegin = bloomFilter.byteCount();
        long fileSize = FileChannelUtils.size(indexChannel);

        while (dataBegin < fileSize - 1) {
            int len = FileChannelUtils.readInt(indexChannel, dataBegin);
            byte[] indexData = FileChannelUtils.read(indexChannel, dataBegin + INT_LENGTH, len);
            dbIndexList.add(DbIndex.from(indexData, dataBegin));
            dataBegin += INT_LENGTH + len;
        }
    }

    public void append(byte[] key, byte[] column, Deque<VersionalValue> values) {
        for(VersionalValue value : values) {
            DbKey dbKey = new DbKey(key, column, value.timestamp());
            DbEntry dbEntry = new DbEntry(dbKey, value.value());
            append(dbEntry);
        }
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

    public boolean delete(DbKey dbKey) {
        Integer globalIndex = findValueGlobalIndex(dbKey);
        if(globalIndex == null) {
            return false;
        }

        valueLogGroup.setExtraData(globalIndex, DELETED_ARRAY);

        return false;
    }

    private void append(DbEntry entry) {
        DbKey dbKey = entry.key();
        bloomFilter.setObj(dbKey);

        long dataBegin = FileChannelUtils.size(indexChannel);
        int globalIndex = valueLogGroup.append(EXISTED_ARRAY, entry.value());
        DbIndex index = new DbIndex(dbKey, dataBegin, globalIndex);
        FileChannelUtils.write(indexChannel, index.toBytes());
        FileChannelUtils.force(indexChannel, false);

        dbIndexList.add(index);

        minDbKey = minDbKey == null ? dbKey : minDbKey.compareTo(dbKey) > 0 ? dbKey : minDbKey;
        maxDbKey = maxDbKey == null ? dbKey : maxDbKey.compareTo(dbKey) < 0 ? dbKey : maxDbKey;
    }

    private Integer findValueGlobalIndex(DbKey dbKey) {
        if(dbKey.compareTo(minDbKey) < 0 || dbKey.compareTo(maxDbKey) > 0) {
            return null;
        }

        if(bloomFilter.isNotExist(dbKey)) {
            return null;
        }

        List<DbIndex> values = dbIndexList.stream()
                .filter(dbIndex -> {
                    DbKey dbIndexKey = dbIndex.key();
                    return Arrays.equals(dbIndexKey.key(), dbKey.key())
                            && Arrays.equals(dbIndexKey.column(), dbKey.column());
                }).sorted(Comparator.comparingLong(o -> o.key().timestamp()))
                .toList();

        int globalIndex = 0;
        long timestamp = dbKey.timestamp();

        if(timestamp == Version.LATEST_VERSION) {
            globalIndex = values.get(values.size() - 1).valueGlobalIndex();
        } else {
            Optional<DbIndex> optional = values.stream()
                    .filter(dbIndex -> timestamp == dbIndex.key().timestamp())
                    .findFirst();

            if(optional.isPresent()) {
                globalIndex = optional.get().valueGlobalIndex();
            }
        }

        return globalIndex;
    }
}
