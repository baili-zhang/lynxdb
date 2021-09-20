package moonlight.engine;

public interface Cacheable<K, V> {

    public void set(K key, V value);
    public V get(K key);
    public void update(K key, V value);
    public void delete(K key);
}
