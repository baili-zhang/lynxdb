package com.bailizhang.lynxdb.lsmtree.common;

import com.bailizhang.lynxdb.lsmtree.exception.ColumnFamilyNotFoundException;

/**
 * 支持：列族，列，时间戳版本
 */
public interface LsmTree {
    byte[] find(byte[] key, byte[] columnFamily, byte[] column, long timestamp) throws ColumnFamilyNotFoundException;
    void insert(byte[] key, byte[] columnFamily, byte[] column, long timestamp, byte[] value) throws ColumnFamilyNotFoundException;
    void delete(byte[] key, byte[] columnFamily, byte[] column, long timestamp) throws ColumnFamilyNotFoundException;
    void addColumn(byte[] columnFamily, byte[] column) throws ColumnFamilyNotFoundException;
    void removeColumn(byte[] columnFamily, byte[] column) throws ColumnFamilyNotFoundException;
}
