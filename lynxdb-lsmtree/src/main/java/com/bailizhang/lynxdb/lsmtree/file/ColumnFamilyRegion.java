package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static com.bailizhang.lynxdb.lsmtree.common.DbKey.*;

public class ColumnFamilyRegion {
    private static final String WAL_DIR = "wal";
    private static final int EXTRA_DATA_LENGTH = 1;

    private final Options options;

    private final LogGroup walLog;

    private MemTable immutable;
    private MemTable mutable;
    private final LevelTree levelTree;

    public ColumnFamilyRegion(String dir, String columnFamily, Options options) {
        this.options = options;

        File file = FileUtils.createDirIfNotExisted(dir, columnFamily);
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

    public byte[] find(DbKey dbKey) throws DeletedException {
        byte[] value = mutable.find(dbKey);
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

    public List<DbValue> find(byte[] key) {
        HashSet<DbValue> dbValues = new HashSet<>();

        if(mutable != null) {
            mutable.find(key, dbValues);
        }

        if(immutable != null) {
            immutable.find(key, dbValues);
        }

        levelTree.find(key, dbValues);

        return dbValues.stream()
                .filter(dbValue -> dbValue.value() != null)
                .toList();
    }

    public HashMap<Key, HashSet<DbValue>> findAll() {
        // TODO
        return null;
    }

    public void insert(DbEntry dbEntry) {
        int walGlobalIndex = -1;
        if(options.wal()) {
            walGlobalIndex = walLog.append(EXISTED_ARRAY, dbEntry);
        }

        insertIntoMemTableAndMerge(dbEntry, walGlobalIndex);
    }

    public void delete(DbKey dbKey) {
        int walGlobalIndex = -1;
        if(options.wal()) {
            walGlobalIndex = walLog.append(DELETED_ARRAY, dbKey.toBytes());
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
            byte[] value = flag == DELETED ? null : BufferUtils.getBytes(buffer);

            DbKey dbKey = new DbKey(key, column, flag);
            DbEntry dbEntry = new DbEntry(dbKey, value);

            if (flag != EXISTED && flag != DELETED) {
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
