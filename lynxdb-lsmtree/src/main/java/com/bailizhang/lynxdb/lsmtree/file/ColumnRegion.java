package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.entry.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.entry.WalEntry;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import static com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion.COLUMNS_DIR;

public class ColumnRegion {
    private static final String WAL_DIR = "wal";
    private final static String VALUE_DIR = "value";
    private static final int EXTRA_DATA_LENGTH = 1;
    private static final int WAL_EXTRA_DATA_LENGTH = 0;

    private final String columnFamily;
    private final String column;

    private final LsmTreeOptions options;

    private final LogGroup walLog;
    private final LogGroup valueLog;

    private MemTable immutable;
    private MemTable mutable;
    private final LevelTree levelTree;

    public ColumnRegion(String columnFamily, String column, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.column = column;
        this.options = options;

        String baseDir = options.baseDir();
        File file = FileUtils.createDirIfNotExisted(baseDir, columnFamily, COLUMNS_DIR, column);
        String dir = file.getAbsolutePath();

        LogGroupOptions valueLogGroupOptions = new LogGroupOptions(EXTRA_DATA_LENGTH);

        // 初始化 value log group
        String valueLogPath = Path.of(dir, VALUE_DIR).toString();
        valueLog = new LogGroup(valueLogPath, valueLogGroupOptions);

        mutable = new MemTable(options);
        levelTree = new LevelTree(dir, options);

        if(options.wal()) {
            // 初始化 wal log group
            String walDir = Path.of(dir, WAL_DIR).toString();
            LogGroupOptions logOptions = new LogGroupOptions(WAL_EXTRA_DATA_LENGTH);
            logOptions.regionCapacity(options.memTableSize());

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
            value = immutable.find(key);
            if(value != null) {
                return value;
            }
        }

        return levelTree.find(key);
    }

    public void insert(byte[] key, byte[] value) {
        // TODO: extraData 用来以后清除 value log group 中的无效数据
        int valueGlobalIndex = valueLog.append(KeyEntry.EXISTED_ARRAY, value);
        KeyEntry keyEntry = KeyEntry.from(KeyEntry.EXISTED, key, value, valueGlobalIndex);

        int maxWalGlobalIndex = -1;
        if(options.wal()) {
            WalEntry walEntry = WalEntry.from(KeyEntry.EXISTED, key, value, valueGlobalIndex);
            byte[] data = walEntry.toBytes();
            maxWalGlobalIndex = walLog.append(ByteArrayUtils.EMPTY_BYTES, data);
        }

        insertIntoMemTableAndMerge(keyEntry, maxWalGlobalIndex);
    }

    public void delete(byte[] key) {
        KeyEntry keyEntry = KeyEntry.from(
                KeyEntry.DELETED,
                key,
                ByteArrayUtils.EMPTY_BYTES,
                -1
        );

        int maxWalGlobalIndex = -1;
        if(options.wal()) {
            WalEntry walEntry = WalEntry.from(
                    KeyEntry.DELETED,
                    key,
                    ByteArrayUtils.EMPTY_BYTES,
                    -1
            );
            byte[] data = walEntry.toBytes();
            maxWalGlobalIndex = walLog.append(ByteArrayUtils.EMPTY_BYTES, data);
        }

        insertIntoMemTableAndMerge(keyEntry, maxWalGlobalIndex);
    }

    private void insertIntoMemTableAndMerge(KeyEntry keyEntry, int maxWalGlobalIndex) {
        if(mutable.full()) {
            MemTable needMerged = immutable;
            mutable.transformToImmutable();
            immutable = mutable;
            mutable = new MemTable(options);
            levelTree.merge(needMerged);

            // TODO: 极端情况下会出现数据重复
            if(options.wal() && needMerged != null) {
                walLog.deleteOldThan(maxWalGlobalIndex - options.memTableSize() + 1);
            }
        }
        mutable.append(keyEntry);
    }

    public boolean existKey(byte[] key) {
        try {
            return mutable.existKey(key)
                    || (immutable != null && immutable.existKey(key))
                    || levelTree.existKey(key);
        } catch (DeletedException e) {
            return false;
        }
    }

    public List<byte[]> rangeNext(byte[] beginKey, int limit) {
        HashSet<Key> existedKeys = new HashSet<>();
        HashSet<Key> deletedKeys = new HashSet<>();

        List<Key> mKeys = mutable.rangeNext(
                beginKey,
                limit,
                deletedKeys,
                existedKeys
        );

        List<Key> imKeys = immutable == null
                ? new ArrayList<>()
                : immutable.rangeNext(beginKey, limit, deletedKeys, existedKeys);

        List<Key> lKeys = levelTree.rangeNext(
                beginKey,
                limit,
                deletedKeys,
                existedKeys
        );

        PriorityQueue<Key> priorityQueue = new PriorityQueue<>();
        priorityQueue.addAll(mKeys);
        priorityQueue.addAll(imKeys);
        priorityQueue.addAll(lKeys);

        List<Key> range = new ArrayList<>();

        for(int i = 0; i < limit; i ++) {
            Key key = priorityQueue.poll();

            if(key == null) {
                break;
            }

            range.add(key);
        }

        range.sort(Key::compareTo);
        return range.stream().map(Key::bytes).toList();
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String column() {
        return column;
    }

    private void recoverFromWal() {
        for(LogEntry entry : walLog) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());

            WalEntry walEntry = WalEntry.from(buffer);
            KeyEntry keyEntry = KeyEntry.from(walEntry);

            insertIntoMemTableAndMerge(keyEntry, -1);
        }
    }
}
