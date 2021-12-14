package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.engine.Cacheable;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache implements Cacheable {
    private static SimpleCache cache = new SimpleCache();

    private ConcurrentHashMap<String, ByteBuffer> stringHashMap = new ConcurrentHashMap<>();

    private SimpleCache() {}

    public static SimpleCache getInstance() {
        return cache;
    }

    @Override
    public void set(String key, ByteBuffer value) {
        stringHashMap.put(key, value);
    }

    @Override
    public ByteBuffer get(String key) {
        return stringHashMap.get(key);
    }

    @Override
    public void update(String key, ByteBuffer value) {
        stringHashMap.put(key, value);
    }

    @Override
    public void delete(String key) {
        stringHashMap.remove(key);
    }
}
