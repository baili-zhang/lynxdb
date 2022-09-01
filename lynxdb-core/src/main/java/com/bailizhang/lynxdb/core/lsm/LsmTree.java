package com.bailizhang.lynxdb.core.lsm;

import com.bailizhang.lynxdb.core.common.Key;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class LsmTree implements Map<Key, byte[]> {
    private static final int MAX_MUTABLE_TABLE_SIZE = 100;

    private static final byte[] NULL_VALUE = new byte[0];

    private final List<List<SSTable>> diskTree = new ArrayList<>();

    private ConcurrentSkipListMap<Key, byte[]> immutableMemTable = new ConcurrentSkipListMap<>();
    private ConcurrentSkipListMap<Key, byte[]> memTable = new ConcurrentSkipListMap<>();

    public LsmTree() {

    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public byte[] get(Object key) {
        byte[] value = memTable.get(key);
        if(value != null) {
            return Arrays.equals(value, NULL_VALUE) ? null : value;
        }

        value = immutableMemTable.get(key);
        if(value != null) {
            return Arrays.equals(value, NULL_VALUE) ? null : value;
        }

        for (List<SSTable> level : diskTree) {
            for (SSTable table : level) {
                if (table.containsKey(key) && (value = table.get(key)) != null) {
                    return value;
                }
            }
        }

        return null;
    }

    @Override
    public byte[] put(Key key, byte[] value) {
        memTable.put(key, value);
        if(memTable.size() == MAX_MUTABLE_TABLE_SIZE) {
            immutableMemTable = memTable;
            /* TODO:异步执行 compact */
            memTable = new ConcurrentSkipListMap<>();
        }
        return null;
    }

    @Override
    public byte[] remove(Object key) {
        if(!(key instanceof Key k)) {
            throw new RuntimeException("key is not an instance of [Key]");
        }
        put(k, NULL_VALUE);
        return null;
    }

    @Override
    public void putAll(Map<? extends Key, ? extends byte[]> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Key> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<byte[]> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<Key, byte[]>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
