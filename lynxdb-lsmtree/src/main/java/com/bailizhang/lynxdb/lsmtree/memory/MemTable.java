package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.lsmtree.entry.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    public byte[] find(byte[] k) throws DeletedException {
        Key key = new Key(k);

        KeyEntry keyEntry = skipListMap.get(key);

        if(keyEntry == null) {
            return null;
        }

        if(keyEntry.flag() == KeyEntry.DELETED) {
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

    public boolean existKey(byte[] key) throws DeletedException {
        KeyEntry entry = skipListMap.get(new Key(key));

        if(entry == null) {
            return false;
        }

        if(entry.flag() == KeyEntry.DELETED) {
            throw new DeletedException();
        }

        return true;
    }

    public List<Key> range(
            byte[] beginKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        Key key = new Key(beginKey);

        List<Key> keys = new ArrayList<>();

        while(limit > 0) {
            Map.Entry<Key, KeyEntry> entry = skipListMap.higherEntry(key);

            if(entry == null) {
                break;
            }

            KeyEntry keyEntry = entry.getValue();
            key = entry.getKey();

            if(keyEntry.flag() == KeyEntry.DELETED) {
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
}
