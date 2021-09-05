package rcache.engine;

import java.util.concurrent.ConcurrentHashMap;

public class StringHashTable implements AbstractEngine<String, String> {

    private static ConcurrentHashMap stringHashMap = new ConcurrentHashMap<String, String>();

    @Override
    public void set(String key, String value) {
        stringHashMap.put(key, value);
    }

    @Override
    public String get(String key) {
        return (String)stringHashMap.get(key);
    }

    @Override
    public void delete(String key) {
        stringHashMap.remove(key);
    }
}
