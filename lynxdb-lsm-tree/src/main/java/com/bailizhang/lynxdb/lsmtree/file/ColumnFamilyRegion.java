package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.io.File;

public class ColumnFamilyRegion {
    private static final int EXTRA_DATA_LENGTH = 0;

    private static final byte INSERT = (byte) 0x01;
    private static final byte DELETE = (byte) 0x02;


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
        walLog = new LogGroup(cfDir, logOptions);
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
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(INSERT);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
        bytesList.appendRawLong(timestamp);
        bytesList.appendRawBytes(value);

        walLog.append(BufferUtils.EMPTY_BYTES, bytesList.toBytes());

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
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(DELETE);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
        bytesList.appendRawLong(timestamp);

        walLog.append(BufferUtils.EMPTY_BYTES, bytesList.toBytes());

        return mutable.delete(key, column, timestamp)
                || immutable.delete(key, column, timestamp)
                || levelTree.delete(key, column, timestamp);
    }
}
