package rcache.engine;

import rcache.exception.KeysException;

import java.util.concurrent.ConcurrentHashMap;

public class StringHashTable implements AbstractStorage<String, String> {

    private static ConcurrentHashMap stringHashMap = new ConcurrentHashMap<String, String>();

    @Override
    public void set(String key, String value) throws KeysException {
        if(stringHashMap.containsKey(key)) {
            throw new KeysException();
        }
        stringHashMap.put(key, value);
    }

    @Override
    public String get(String key) throws KeysException {
        if(!stringHashMap.containsKey(key)) {
            throw new KeysException();
        }
        return (String)stringHashMap.get(key);
    }

    @Override
    public void update(String key, String value) throws KeysException {
        if(!stringHashMap.containsKey(key)) {
            throw new KeysException();
        }
        stringHashMap.put(key, value);
    }

    @Override
    public void delete(String key) throws KeysException {
        if(!stringHashMap.containsKey(key)) {
            throw new KeysException();
        }
        stringHashMap.remove(key);
    }
}
