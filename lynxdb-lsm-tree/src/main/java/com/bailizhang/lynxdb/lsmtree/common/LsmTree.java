package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.lsmtree.exception.ColumnFamilyNotFoundException;

/**
 * 支持：列族，列，时间戳版本
 */
public interface LsmTree {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column, long timestamp) throws ColumnFamilyNotFoundException;
    void insert(byte[] key, byte[] columnFamily, byte[] column, long timestamp, byte[] value) throws ColumnFamilyNotFoundException;
    boolean delete(byte[] key, byte[] columnFamily, byte[] column, long timestamp) throws ColumnFamilyNotFoundException;
}
