package rcache.engine;

public interface AbstractEngine<K, V> {

    public void set(K key, V value);
    public V get(K key);
    public void delete(K key);
}
