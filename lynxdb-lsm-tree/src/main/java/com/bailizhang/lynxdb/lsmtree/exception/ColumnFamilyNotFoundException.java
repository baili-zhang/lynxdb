package com.bailizhang.lynxdb.lsmtree.exception;

public class ColumnFamilyNotFoundException extends RuntimeException {
    public ColumnFamilyNotFoundException(byte[] columnFamily) {

    }
}
