package zbl.moonlight.server.engine.simple;

import java.util.LinkedHashMap;
import java.util.Map;

public class SimpleLRU<K,V> extends LinkedHashMap<K,V> implements Map<K,V> {
    private final int capacity;

    public SimpleLRU(int capacity) {
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        return size() > capacity;
    }
}
