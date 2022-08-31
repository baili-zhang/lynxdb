package com.bailizhang.lynxdb.storage.core;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class SingleTableRow implements Map<Column, byte[]> {
    private final byte[] rowKey;
    private final Map<Column, byte[]> row = new LinkedHashMap<>();

    public SingleTableRow(byte[] key) {
        rowKey = key;
    }

    public byte[] rowKey() {
        return rowKey;
    }

    @Override
    public int size() {
        return row.size();
    }

    @Override
    public boolean isEmpty() {
        return row.isEmpty();
    }

    @Override
    public boolean containsKey(Object column) {
        return row.containsKey(column);
    }

    @Override
    public boolean containsValue(Object value) {
        return row.containsValue(value);
    }

    @Override
    public byte[] get(Object column) {
        return row.get(column);
    }

    @Override
    public byte[] put(Column column, byte[] value) {
        return row.put(column, value);
    }

    @Override
    public byte[] remove(Object column) {
        return row.remove(column);
    }

    @Override
    public void putAll(Map<? extends Column, ? extends byte[]> m) {
        row.putAll(m);
    }

    @Override
    public void clear() {
        row.clear();
    }

    @Override
    public Set<Column> keySet() {
        return row.keySet();
    }

    @Override
    public Collection<byte[]> values() {
        return row.values();
    }

    @Override
    public Set<Entry<Column, byte[]>> entrySet() {
        return row.entrySet();
    }
}
