package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.engine.Cacheable;

import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache implements Cacheable<String, String> {
    private static SimpleCache cache = new SimpleCache();

    private ConcurrentHashMap stringHashMap = new ConcurrentHashMap<String, String>();

    private SimpleCache() {}

    public static SimpleCache getInstance() {
        return cache;
    }

    @Override
    public void set(String key, String value) {
        stringHashMap.put(key, value);
    }

    @Override
    public String get(String key) {
        return (String)stringHashMap.get(key);
    }

    @Override
    public void update(String key, String value) {
        stringHashMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        stringHashMap.remove(key);
    }
}
