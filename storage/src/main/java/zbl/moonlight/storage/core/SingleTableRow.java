package zbl.moonlight.storage.core;

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
    public boolean containsKey(Object key) {
        return row.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return row.containsValue(value);
    }

    @Override
    public byte[] get(Object key) {
        return row.get(key);
    }

    @Override
    public byte[] put(Column key, byte[] value) {
        return row.put(key, value);
    }

    @Override
    public byte[] remove(Object key) {
        return row.remove(key);
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
