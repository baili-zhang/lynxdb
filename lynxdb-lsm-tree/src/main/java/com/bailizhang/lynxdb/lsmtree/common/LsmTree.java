package com.bailizhang.lynxdb.lsmtree.common;

/**
 * 支持：列族，列，时间戳版本
 */
public interface LsmTree {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column, long timestamp);
    void insert(byte[] key, byte[] columnFamily, byte[] column, long timestamp, byte[] value);
    boolean delete(byte[] key, byte[] columnFamily, byte[] column, long timestamp);
}
