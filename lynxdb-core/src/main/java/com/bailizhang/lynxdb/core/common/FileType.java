package com.bailizhang.lynxdb.core.common;

public enum FileType {
    LOG_GROUP_FILE(1001, "lg"),
    LOG_GROUP_REGION_FILE(1002, "lgr"),
    SSTABLE_FILE(2001, "sst");

    private final int magicNumber;
    private final String suffix;

    FileType(int magicNumber, String suffix) {
        this.magicNumber = magicNumber;
        this.suffix = suffix;
    }

    public int getMagicNumber() {
        return magicNumber;
    }

    public String getSuffix() {
        return suffix;
    }
}
