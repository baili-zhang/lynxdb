package zbl.moonlight.storage.core;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class MultiTableRows implements Map<byte[], Map<Column, byte[]>> {
    private final Map<byte[], Map<Column, byte[]>> rows = new LinkedHashMap<>();

    public MultiTableRows() {
    }

    @Override
    public int size() {
        return rows.size();
    }

    @Override
    public boolean isEmpty() {
        return rows.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return rows.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return rows.containsValue(value);
    }

    @Override
    public Map<Column, byte[]> get(Object key) {
        return rows.get(key);
    }

    @Override
    public Map<Column, byte[]> put(byte[] key, Map<Column, byte[]> value) {
        return rows.put(key, value);
    }

    @Override
    public Map<Column, byte[]> remove(Object key) {
        return rows.remove(key);
    }

    @Override
    public void putAll(Map<? extends byte[], ? extends Map<Column, byte[]>> m) {
        rows.putAll(m);
    }

    @Override
    public void clear() {
        rows.clear();
    }

    @Override
    public Set<byte[]> keySet() {
        return rows.keySet();
    }

    @Override
    public Collection<Map<Column, byte[]>> values() {
        return rows.values();
    }

    @Override
    public Set<Entry<byte[], Map<Column, byte[]>>> entrySet() {
        return rows.entrySet();
    }
}
