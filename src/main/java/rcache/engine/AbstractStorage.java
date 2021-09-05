package rcache.engine;

import rcache.exception.KeysException;

public interface AbstractStorage<K, V> {

    public void set(K key, V value) throws KeysException;
    public V get(K key) throws KeysException;
    public void update(K key, V value) throws KeysException;
    public void delete(K key) throws KeysException;
}
