package com.bailizhang.lynxdb.core.common;

public enum FileType {
    LOG_GROUP_MANAGE_FILE(1001, ".lgm"),
    LOG_GROUP_REGION_FILE(1002, ".lgr"),
    SSTABLE_FILE(2001, ".sst");

    private final int magicNumber;
    private final String suffix;

    FileType(int magicNumber, String suffix) {
        this.magicNumber = magicNumber;
        this.suffix = suffix;
    }

    public int magicNumber() {
        return magicNumber;
    }

    public String suffix() {
        return suffix;
    }
}
