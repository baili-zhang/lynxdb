package com.bailizhang.lynxdb.lsmtree.common;

import java.util.List;

/**
 * 支持：列族，列
 */
public interface LsmTree {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column);
    List<DbValue> find(byte[] key, byte[] columnFamily);
    void insert(byte[] key, byte[] columnFamily, byte[] column, byte[] value);
    boolean delete(byte[] key, byte[] columnFamily, byte[] column);
    void clear();
}
