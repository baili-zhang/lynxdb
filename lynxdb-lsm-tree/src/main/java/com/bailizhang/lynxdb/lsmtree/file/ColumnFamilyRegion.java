package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.log.WriteAheadLog;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.nio.file.Path;

public class ColumnFamilyRegion {
    private final WriteAheadLog wal;
    private MemTable immutable;
    private MemTable mutable;
    private final LevelTree levelTree;

    public ColumnFamilyRegion(String dir, String columnFamily) {
        Path path = Path.of(dir, columnFamily);
        String cfDir = path.toString();

        wal = new WriteAheadLog(cfDir);
        mutable = new MemTable();
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
            mutable = new MemTable();
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
