package com.bailizhang.lynxdb.storage.core;

import java.util.List;

public interface KvAdapter extends AutoCloseable {
    byte[] get(byte[] key);
    List<Pair<byte[], byte[]>> get(List<byte[]> keys);

    void set(Pair<byte[], byte[]> kvPair);
    void set(List<Pair<byte[], byte[]>> kvPairs);

    void delete(byte[] key);
    void delete(List<byte[]> keys);

    void ValueInsert(byte[] key, List<byte[]> values);

    Snapshot snapshot();
}
