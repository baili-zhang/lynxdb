package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileChannelUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.*;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;
import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.DELETED_ARRAY;
import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.EXISTED_ARRAY;

public class SsTable {
    private final FileChannel fileChannel;
    private final BloomFilter bloomFilter;
    private final LogGroup valueLogGroup;
    private final List<DbIndex> dbIndexList;

    public SsTable(String dir, int id, int levelNo, LogGroup logGroup, Options options) {
        int ssTableSize = options.memTableSize() * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, levelNo);

        String filename = NameUtils.name(id);
        FileUtils.createFileIfNotExisted(dir, filename);

        Path filePath = Path.of(dir, filename);

        fileChannel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.APPEND
        );

        FileChannel readChannel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.READ
        );

        bloomFilter = new BloomFilter(filePath, ssTableSize);

        valueLogGroup = logGroup;
        dbIndexList = new ArrayList<>(ssTableSize);

        // 从持久化的文件中恢复数据
        int dataBegin = bloomFilter.byteCount();
        long fileSize = FileChannelUtils.size(fileChannel);

        while (dataBegin < fileSize - 1) {
            int len = FileChannelUtils.readInt(readChannel, dataBegin);
            byte[] indexData = FileChannelUtils.read(readChannel, dataBegin + INT_LENGTH, len);
            dbIndexList.add(DbIndex.from(indexData, dataBegin));
            dataBegin += INT_LENGTH + len;
        }
    }

    public void append(byte[] key, byte[] column, byte[] value) {
        DbKey dbKey = new DbKey(key, column);
        DbEntry dbEntry = new DbEntry(dbKey, value);

        int globalIndex = valueLogGroup.append(EXISTED_ARRAY, dbEntry.value());

        append(dbKey, globalIndex);
    }

    public void append(DbKey dbKey, int globalIndex) {
        bloomFilter.setObj(dbKey);
        long dataBegin = FileChannelUtils.size(fileChannel);
        DbIndex index = new DbIndex(dbKey, dataBegin, globalIndex);
        FileChannelUtils.write(fileChannel, index.toBytes());
        FileChannelUtils.force(fileChannel, false);

        dbIndexList.add(index);
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

        int globalIndex = 0;

        if(optional.isPresent()) {
            globalIndex = optional.get().valueGlobalIndex();
        }

        return globalIndex;
    }
}
