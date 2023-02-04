package com.bailizhang.lynxdb.lsmtree.config;

public class Options {
    private static final int DEFAULT_MEM_TABLE_SIZE = 2000;

    private final int memTableSize;
    private boolean wal = true;

    public Options() {
        this(DEFAULT_MEM_TABLE_SIZE);
    }

    public Options(int memTableSize) {
        this.memTableSize = memTableSize;
    }

    public int memTableSize() {
        return memTableSize;
    }

    public boolean wal() {
        return wal;
    }

    public void wal(boolean val) {
        wal = val;
    }
}
