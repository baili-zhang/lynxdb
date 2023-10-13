package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.entry.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.exception.TimeoutException;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    private final LsmTreeOptions options;
    private volatile boolean immutable = false;

    /**
     * KeyEntry 保存了是否是删除的 flag 信息
     */
    private final ConcurrentSkipListMap<Key, KeyEntry> skipListMap = new ConcurrentSkipListMap<>();

    public MemTable(LsmTreeOptions options) {
        this.options = options;
    }

    public void append(KeyEntry keyEntry) {
        if(immutable) {
            return;
        }

        byte[] key = keyEntry.key();
        skipListMap.put(new Key(key), keyEntry);
    }

    public byte[] find(byte[] k) throws DeletedException, TimeoutException {
        Key key = new Key(k);

        KeyEntry keyEntry = skipListMap.get(key);

        if(keyEntry == null) {
            return null;
        }

        if(keyEntry.isTimeout()) {
            throw new TimeoutException();
        }

        if(keyEntry.flag() == Flags.DELETED) {
            throw new DeletedException();
        }

        return keyEntry.value();
    }

    public boolean full() {
        return skipListMap.size() >= options.memTableSize();
    }

    public void transformToImmutable() {
        immutable = true;
    }

    /**
     * 合并到 memTable 时用的
     *
     * @return DB entries
     */
    public List<KeyEntry> all() {
        // TODO: values 是不是排序过的？
        return new ArrayList<>(skipListMap.values());
    }

    public boolean existKey(byte[] key) throws DeletedException, TimeoutException {
        KeyEntry entry = skipListMap.get(new Key(key));

        if(entry == null) {
            return false;
        }

        if(entry.flag() == Flags.DELETED) {
            throw new DeletedException();
        }

        if(entry.isTimeout()) {
            throw new TimeoutException();
        }

        return true;
    }

    public List<Key> rangeNext(
            byte[] beginKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        return range(
                beginKey,
                limit,
                deletedKeys,
                existedKeys,
                skipListMap::higherEntry
        );
    }

    public List<Key> rangeBefore(
            byte[] endKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        return range(
                endKey,
                limit,
                deletedKeys,
                existedKeys,
                skipListMap::lowerEntry
        );
    }

    private List<Key> range(
            byte[] baseKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys,
            RangeOperator operator
    ) {
        Key key = new Key(baseKey);

        List<Key> keys = new ArrayList<>();

        while(limit > 0) {
            Map.Entry<Key, KeyEntry> entry = operator.doRange(key);

            if(entry == null) {
                break;
            }

            KeyEntry keyEntry = entry.getValue();
            key = entry.getKey();

            if(keyEntry.flag() == Flags.DELETED) {
                deletedKeys.add(key);
                continue;
            }

            if(deletedKeys.contains(key) || existedKeys.contains(key)) {
                continue;
            }

            keys.add(key);
            existedKeys.add(key);

            limit --;
        }

        return keys;
    }

    @FunctionalInterface
    private interface RangeOperator {
        Map.Entry<Key, KeyEntry> doRange(Key key);
    }
}
