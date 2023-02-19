package com.bailizhang.lynxdb.lsmtree;

import java.util.HashMap;

/**
 * 支持：列族，列
 */
public interface Table {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column);
    HashMap<byte[], byte[]> find(byte[] key, byte[] columnFamily);
    HashMap<byte[], HashMap<byte[], byte[]>> findAll(byte[] columnFamily);

    void insert(byte[] key, byte[] columnFamily, byte[] column, byte[] value);
    void insert(byte[] key, byte[] columnFamily, HashMap<byte[], byte[]> multiColumns);

    void delete(byte[] key, byte[] columnFamily, byte[] column);
    void delete(byte[] key, byte[] columnFamily);
    boolean existKey(byte[] key, byte[] columnFamily);
    void clear();
}
