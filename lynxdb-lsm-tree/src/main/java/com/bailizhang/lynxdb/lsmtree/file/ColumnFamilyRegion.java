package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;

public class ColumnFamilyRegion {
    public static final byte EXISTED = (byte) 0x01;
    public static final byte DELETED = (byte) 0x02;

    public static final byte[] EXISTED_ARRAY = new byte[]{EXISTED};
    public static final byte[] DELETED_ARRAY = new byte[]{DELETED};

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
        walLog = new LogGroup(walDir, logOptions);
        mutable = new MemTable(options);
        levelTree = new LevelTree(cfDir, options);

        recoverFromWal();
    }

    public byte[] find(DbKey dbKey) {
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

    public void insert(DbEntry dbEntry) {
        walLog.append(EXISTED_ARRAY, dbEntry);
        insertIntoMemTableAndMerge(dbEntry);
    }

    public boolean delete(DbKey dbKey) {
        walLog.append(DELETED_ARRAY, dbKey.toBytes());
        return deleteFromMemTableAndLevelTree(dbKey);
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

    private boolean deleteFromMemTableAndLevelTree(DbKey dbKey) {
        return mutable.delete(dbKey)
                || immutable.delete(dbKey)
                || levelTree.delete(dbKey);
    }

    private void recoverFromWal() {
        for(LogEntry entry : walLog) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());

            byte flag = buffer.get();
            byte[] key = BufferUtils.getBytes(buffer);
            byte[] column = BufferUtils.getBytes(buffer);
            byte[] value = BufferUtils.getRemaining(buffer);

            DbKey dbKey = new DbKey(key, column);
            DbEntry dbEntry = new DbEntry(dbKey, value);

            switch (flag) {
                case EXISTED -> insertIntoMemTableAndMerge(dbEntry);
                case DELETED -> deleteFromMemTableAndLevelTree(dbKey);
                default -> throw new RuntimeException();
            }
        }
    }
}
