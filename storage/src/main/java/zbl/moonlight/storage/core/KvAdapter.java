package zbl.moonlight.storage.core;

import java.util.List;

public interface KvAdapter {
    byte[] get(byte[] key);
    List<byte[]> get(List<byte[]> keys);
    void set(Pair<byte[], byte[]> kvPair);
    void set(List<Pair<byte[], byte[]>> kvPairs);
    void delete(byte[] key);
    void delete(List<byte[]> keys);
}
