package com.bailizhang.lynxdb.lsmtree.common;

public class Options {
    private static final int DEFAULT_MEM_TABLE_SIZE = 2000;

    private boolean createColumnFamilyIfNotExisted = false;
    private final int memTableSize;

    public Options() {
        this(DEFAULT_MEM_TABLE_SIZE);
    }

    public Options(int memTableSize) {
        this.memTableSize = memTableSize;
    }

    public void createColumnFamilyIfNotExisted(boolean val) {
        createColumnFamilyIfNotExisted = val;
    }

    public boolean createColumnFamilyIfNotExisted() {
        return createColumnFamilyIfNotExisted;
    }

    public int memTableSize() {
        return memTableSize;
    }
}
