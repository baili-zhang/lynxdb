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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
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

        LogOptions logOptions = new LogOptions(EXTRA_DATA_LENGTH);
        String walDir = Path.of(cfDir, WAL_DIR).toString();
        mutable = new MemTable(options);
        levelTree = new LevelTree(cfDir, options);

        if(options.wal()) {
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

        mutable.find(key, dbValues);
        immutable.find(key, dbValues);
        levelTree.find(key, dbValues);

        return dbValues.stream()
                .filter(dbValue -> dbValue.value() != null)
                .toList();
    }

    public void insert(DbEntry dbEntry) {
        if(options.wal()) {
            walLog.append(EXISTED_ARRAY, dbEntry);
        }

        insertIntoMemTableAndMerge(dbEntry);
    }

    public void delete(DbKey dbKey) {
        if(options.wal()) {
            walLog.append(DELETED_ARRAY, dbKey.toBytes());
        }

        insertIntoMemTableAndMerge(new DbEntry(dbKey, null));
    }

    private void insertIntoMemTableAndMerge(DbEntry dbEntry) {
        if(mutable.full()) {
            MemTable needMerged = immutable;
            mutable.transformToImmutable();
            immutable = mutable;
            mutable = new MemTable(options);
            levelTree.merge(needMerged);
        }
        mutable.append(dbEntry);
    }

    private void recoverFromWal() {
        for(LogEntry entry : walLog) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());

            byte flag = entry.index().extraData()[0];
            byte[] key = BufferUtils.getBytes(buffer);
            byte[] column = BufferUtils.getBytes(buffer);
            byte[] value = BufferUtils.getBytes(buffer);

            DbKey dbKey = new DbKey(key, column, flag);
            DbEntry dbEntry = new DbEntry(dbKey, value);

            if (flag != EXISTED && flag != DELETED) {
                throw new RuntimeException();
            }

            insertIntoMemTableAndMerge(dbEntry);
        }
    }
}
