package com.bailizhang.lynxdb.lsmtree.common;

import java.util.List;

/**
 * 支持：列族，列，时间戳版本
 */
public interface LsmTree {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column);
    void insert(byte[] key, byte[] columnFamily, byte[] column, byte[] value);
    boolean delete(byte[] key, byte[] columnFamily, byte[] column);
    List<byte[]> range(byte[] begin, byte[] end, byte[] columnFamily);
}
