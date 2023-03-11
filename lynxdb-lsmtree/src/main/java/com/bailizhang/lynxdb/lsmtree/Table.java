package com.bailizhang.lynxdb.lsmtree;

import java.util.HashMap;

/**
 * 支持：列族，列
 */
public interface Table {
    byte[] find(byte[] key, String columnFamily, String column);
    HashMap<String, byte[]> find(byte[] key, String columnFamily);

    HashMap<byte[], HashMap<String, byte[]>> rangeNext(
            String columnFamily,
            String mainColumn,
            byte[] beginKey,
            int limit
    );

    HashMap<byte[], HashMap<String, byte[]>> rangeBefore(
            String columnFamily,
            String mainColumn,
            byte[] beginKey,
            int limit
    );

    void insert(byte[] key, String columnFamily, String column, byte[] value);
    void insert(byte[] key, String columnFamily, HashMap<String, byte[]> multiColumns);

    void delete(byte[] key, String columnFamily, String column);
    void delete(byte[] key, String columnFamily);

    boolean existKey(
            byte[] key,
            String columnFamily,
            String mainColumn
    );

    void clear();
}
