package com.bailizhang.lynxdb.core.log;

public class LogOptions {
    private final int extraDataLength;
    private boolean forceAfterEachAppend = false;
    private boolean forceAfterRegionFull = true;

    public LogOptions(int extraDataLength) {
        this.extraDataLength = extraDataLength;
    }

    public int extraDataLength() {
        return extraDataLength;
    }

    public boolean forceAfterEachAppend() {
        return forceAfterEachAppend;
    }

    public boolean forceAfterRegionFull() {
        return forceAfterRegionFull;
    }
}
