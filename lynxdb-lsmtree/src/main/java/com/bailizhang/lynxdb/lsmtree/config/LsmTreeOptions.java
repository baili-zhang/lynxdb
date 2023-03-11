package com.bailizhang.lynxdb.lsmtree.config;

public class LsmTreeOptions {
    private static final int DEFAULT_MEM_TABLE_SIZE = 2000;
    private static final String BASE_DIR = System.getProperty("user.dir") + "/data";

    private final int memTableSize;

    private final String baseDir;

    private boolean wal = true;

    public LsmTreeOptions() {
        this(BASE_DIR, DEFAULT_MEM_TABLE_SIZE);
    }

    public LsmTreeOptions(String baseDir, int memTableSize) {
        this.baseDir = baseDir;
        this.memTableSize = memTableSize;
    }

    public int memTableSize() {
        return memTableSize;
    }

    public String baseDir() {
        return baseDir;
    }

    public boolean wal() {
        return wal;
    }

    public void wal(boolean val) {
        wal = val;
    }
}
