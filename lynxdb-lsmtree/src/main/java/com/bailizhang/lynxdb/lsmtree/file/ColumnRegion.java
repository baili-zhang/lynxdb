package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

public class ColumnRegion {
    private static final String WAL_DIR = "wal";
    private static final int EXTRA_DATA_LENGTH = 1;

    private final String columnFamily;
    private final String column;

    private final LsmTreeOptions options;

    private final LogGroup walLog;

    private MemTable immutable;
    private MemTable mutable;
    private final LevelTree levelTree;

    public ColumnRegion(String columnFamily, String column, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.column = column;
        this.options = options;

        String baseDir = options.baseDir();
        File file = FileUtils.createDirIfNotExisted(baseDir, columnFamily, column);
        String cfDir = file.getAbsolutePath();

        String walDir = Path.of(cfDir, WAL_DIR).toString();
        mutable = new MemTable(options);
        levelTree = new LevelTree(cfDir, options);

        if(options.wal()) {
            LogOptions logOptions = new LogOptions(EXTRA_DATA_LENGTH);
            logOptions.logRegionSize(options.memTableSize());

            walLog = new LogGroup(walDir, logOptions);
            recoverFromWal();
        } else {
            walLog = null;
        }
    }

    public byte[] find(byte[] key) throws DeletedException {
        byte[] value = mutable.find(key);
        if(value != null) {
            return value;
        }

        if(immutable != null) {
            value = immutable.find(dbKey);
            if(value != null) {
                return value;
            }
        }

        return levelTree.find(dbKey);
    }

    public HashMap<Key, HashSet<DbValue>> findAll() {
        HashMap<Key, HashSet<DbValue>> map = new HashMap<>();

        mutable.findAll(map);
        immutable.findAll(map);

        levelTree.findAll(map);

        return map;
    }

    public void insert(DbEntry dbEntry) {
        int walGlobalIndex = -1;
        if(options.wal()) {
            walGlobalIndex = walLog.append(KeyEntry.EXISTED_ARRAY, dbEntry);
        }

        insertIntoMemTableAndMerge(dbEntry, walGlobalIndex);
    }

    public void delete(KeyEntry dbKey) {
        int walGlobalIndex = -1;
        if(options.wal()) {
            walGlobalIndex = walLog.append(KeyEntry.DELETED_ARRAY, dbKey.toBytes());
        }

        DbEntry dbEntry = new DbEntry(dbKey, null);
        insertIntoMemTableAndMerge(dbEntry, walGlobalIndex);
    }

    private void insertIntoMemTableAndMerge(DbEntry dbEntry, int walGlobalIndex) {
        if(mutable.full()) {
            MemTable needMerged = immutable;
            mutable.transformToImmutable();
            immutable = mutable;
            mutable = new MemTable(options);
            levelTree.merge(needMerged);

            if(options.wal() && needMerged != null) {
                walLog.deleteOldContains(needMerged.maxWalGlobalIndex());
            }
        }
        mutable.append(dbEntry, walGlobalIndex);
    }

    private void recoverFromWal() {
        for(LogEntry entry : walLog) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());

            byte flag = entry.index().extraData()[0];
            byte[] key = BufferUtils.getBytes(buffer);
            byte[] column = BufferUtils.getBytes(buffer);
            byte[] value = flag == KeyEntry.DELETED ? null : BufferUtils.getBytes(buffer);

            KeyEntry dbKey = new KeyEntry(key, column, flag);
            DbEntry dbEntry = new DbEntry(dbKey, value);

            if (flag != KeyEntry.EXISTED && flag != KeyEntry.DELETED) {
                throw new RuntimeException();
            }

            insertIntoMemTableAndMerge(dbEntry, -1);
        }
    }

    public boolean existKey(byte[] key) {
        // TODO
        return false;
    }

    public List<byte[]> findColumns(byte[] key) {
        // TODO
        return null;
    }
}
