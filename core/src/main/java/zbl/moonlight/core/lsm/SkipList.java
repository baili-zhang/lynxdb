package zbl.moonlight.core.lsm;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SkipList implements Map<String, byte[]> {
    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public byte[] get(Object key) {
        return new byte[0];
    }

    @Override
    public byte[] put(String key, byte[] value) {
        return new byte[0];
    }

    @Override
    public byte[] remove(Object key) {
        return new byte[0];
    }

    @Override
    public void putAll(Map<? extends String, ? extends byte[]> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<String> keySet() {
        return null;
    }

    @Override
    public Collection<byte[]> values() {
        return null;
    }

    @Override
    public Set<Entry<String, byte[]>> entrySet() {
        return null;
    }
}
