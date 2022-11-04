package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.log.WriteAheadLog;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.io.File;

public class ColumnFamilyRegion {
    private final Options options;

    private final WriteAheadLog wal;
    private MemTable immutable;
    private MemTable mutable;
    private final LevelTree levelTree;

    public ColumnFamilyRegion(String dir, String columnFamily, Options options) {
        this.options = options;

        File file = FileUtils.createDirIfNotExisted(dir, columnFamily);
        String cfDir = file.getAbsolutePath();

        wal = new WriteAheadLog(cfDir, options);
        mutable = new MemTable(options);
        levelTree = new LevelTree(cfDir);
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        byte[] value = mutable.find(key, column, timestamp);
        if(value != null) {
            return value;
        }

        value = immutable.find(key, column, timestamp);
        if(value != null) {
            return value;
        }

        return levelTree.find(key, column, timestamp);
    }

    public void insert(byte[] key, byte[] column, long timestamp, byte[] value) {
        wal.appendInsert(key, column, timestamp);
        if(mutable.full()) {
            MemTable needMerged = immutable;
            mutable.transformToImmutable();
            immutable = mutable;
            mutable = new MemTable(options);
            levelTree.merge(needMerged);
        }
        mutable.append(key, column, timestamp, value);
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        wal.appendDelete(key, column, timestamp);
        return mutable.delete(key, column, timestamp)
                || immutable.delete(key, column, timestamp)
                || levelTree.delete(key, column, timestamp);
    }
}
