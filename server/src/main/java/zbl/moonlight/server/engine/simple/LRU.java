package zbl.moonlight.server.engine.simple;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRU<K,V> extends LinkedHashMap<K,V> implements Map<K,V> {
    private final long capacity;

    public LRU(long capacity) {
        this.capacity = capacity;
    }

    @Override
    protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
        if(size() > capacity){
            return true;
        }
        return false;
    }
}
