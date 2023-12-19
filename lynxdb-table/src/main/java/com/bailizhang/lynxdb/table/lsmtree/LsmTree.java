package com.bailizhang.lynxdb.table.lsmtree;

import com.bailizhang.lynxdb.core.common.Bytes;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.entry.KeyEntry;
import com.bailizhang.lynxdb.table.entry.WalEntry;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.level.Levels;
import com.bailizhang.lynxdb.table.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.table.schema.Key;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;

import static com.bailizhang.lynxdb.table.columnfamily.ColumnFamilyRegion.COLUMNS_DIR;

public class LsmTree {
    private static final String WAL_DIR = "wal";
    private final static String VALUE_DIR = "value";

    private final String columnFamily;
    private final String column;

    private final LsmTreeOptions options;

    private final LogGroup walLog;
    private final LogGroup valueLog;

    private MemTable immutable;
    private MemTable mutable;
    private final Levels levels;

    public LsmTree(String columnFamily, String column, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.column = column;
        this.options = options;

        String baseDir = options.baseDir();
        File file = FileUtils.createDirIfNotExisted(baseDir, columnFamily, COLUMNS_DIR, column);
        String dir = file.getAbsolutePath();

        LogGroupOptions valueLogGroupOptions = new LogGroupOptions();

        // 初始化 value log group
        String valueLogPath = Path.of(dir, VALUE_DIR).toString();
        valueLog = new LogGroup(valueLogPath, valueLogGroupOptions);

        mutable = new MemTable(options);
        levels = new Levels(dir, valueLog, options);

        if(options.wal()) {
            // 初始化 wal log group
            String walDir = Path.of(dir, WAL_DIR).toString();
            LogGroupOptions logOptions = new LogGroupOptions();
            logOptions.regionCapacity(options.memTableSize());

            walLog = new LogGroup(walDir, logOptions);
            recoverFromWal();
        } else {
            walLog = null;
        }
    }

    public byte[] find(byte[] key) throws DeletedException, TimeoutException {
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

        return levels.find(key);
    }

    public void insert(byte[] key, byte[] value, long timeout) {
        int valueGlobalIndex = valueLog.appendEntry(value);
        KeyEntry keyEntry = KeyEntry.from(
                Flags.EXISTED,
                key,
                value,
                valueGlobalIndex,
                timeout
        );

        int maxWalGlobalIndex = -1;
        if(options.wal()) {
            WalEntry walEntry = WalEntry.from(
                    Flags.EXISTED,
                    key,
                    value,
                    valueGlobalIndex,
                    timeout
            );
            ByteBuffer[] data = walEntry.toBuffers();
            maxWalGlobalIndex = walLog.appendEntry(data);
        }

        insertIntoMemTableAndMerge(keyEntry, maxWalGlobalIndex);
    }

    public void delete(byte[] key) {
        KeyEntry keyEntry = KeyEntry.from(
                Flags.DELETED,
                key,
                Bytes.EMPTY,
                -1,
                0L
        );

        int maxWalGlobalIndex = -1;
        if(options.wal()) {
            WalEntry walEntry = WalEntry.from(
                    Flags.DELETED,
                    key,
                    Bytes.EMPTY,
                    -1,
                    0L
            );
            ByteBuffer[] data = walEntry.toBuffers();
            maxWalGlobalIndex = walLog.appendEntry(data);
        }

        insertIntoMemTableAndMerge(keyEntry, maxWalGlobalIndex);
    }

    private void insertIntoMemTableAndMerge(KeyEntry keyEntry, int maxWalGlobalIndex) {
        if(mutable.full()) {
            MemTable needMerged = immutable;
            mutable.transformToImmutable();
            immutable = mutable;
            mutable = new MemTable(options);
            levels.merge(needMerged);

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
                    || levels.existKey(key);
        } catch (DeletedException | TimeoutException o_0) {
            return false;
        }
    }

    public List<byte[]> rangeNext(byte[] beginKey, int limit) {
        return range(
                beginKey,
                limit,
                Comparator.naturalOrder(),
                mutable::rangeNext,
                immutable == null ? null : immutable::rangeNext,
                levels::rangeNext
        );
    }

    public List<byte[]> rangeBefore(byte[] endKey, int limit) {
        return range(
                endKey,
                limit,
                Comparator.reverseOrder(),
                mutable::rangeBefore,
                immutable == null ? null : immutable::rangeNext,
                levels::rangeBefore
        );
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String column() {
        return column;
    }

    private List<byte[]> range(
            byte[] beginKey,
            int limit,
            Comparator<Key> comparator,
            RangeOperator mutableRangeOperator,
            RangeOperator immutableRangeOperator,
            RangeOperator levelTreeRangeOperator
    ) {
        HashSet<Key> existedKeys = new HashSet<>();
        HashSet<Key> deletedKeys = new HashSet<>();

        List<Key> mKeys = mutableRangeOperator.doRange(
                beginKey,
                limit,
                deletedKeys,
                existedKeys
        );

        List<Key> imKeys = immutableRangeOperator == null
                ? new ArrayList<>()
                : immutableRangeOperator.doRange(beginKey, limit, deletedKeys, existedKeys);

        List<Key> lKeys = levelTreeRangeOperator.doRange(
                beginKey,
                limit,
                deletedKeys,
                existedKeys
        );

        PriorityQueue<Key> priorityQueue = new PriorityQueue<>(comparator);
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

    private void recoverFromWal() {
        for(LogEntry entry : walLog) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());

            WalEntry walEntry = WalEntry.from(buffer);
            KeyEntry keyEntry = KeyEntry.from(walEntry);

            insertIntoMemTableAndMerge(keyEntry, -1);
        }
    }

    @FunctionalInterface
    private interface RangeOperator {
        List<Key> doRange(
                byte[] baseKey,
                int limit,
                HashSet<Key> deletedKeys,
                HashSet<Key> existedKeys
        );
    }
}
