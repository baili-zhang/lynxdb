package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.memory.VersionalValue;

import java.util.Deque;

public class SsTable {
    public void append(byte[] key, byte[] column, Deque<VersionalValue> values) {

    }

    public boolean isBiggerThan(byte[] key, byte[] column) {
        return false;
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        return null;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        return false;
    }
}
