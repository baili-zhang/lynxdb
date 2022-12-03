package com.bailizhang.lynxdb.core.log;

public class LogOptions {
    private final int extraDataLength;
    private Integer logRegionSize;
    private boolean forceAfterEachAppend = false;
    private boolean forceAfterRegionFull = true;

    public LogOptions(int extraDataLength) {
        this.extraDataLength = extraDataLength;
        this.logRegionSize = null;
    }

    public int extraDataLength() {
        return extraDataLength;
    }

    public void logRegionSize(int val) {
        if(val <= 0 || logRegionSize != null) {
            return;
        }

        logRegionSize = val;
    }

    public Integer logRegionSize() {
        return logRegionSize;
    }

    public void forceAfterEachAppend(boolean val) {
        forceAfterEachAppend = val;
        forceAfterRegionFull = false;
    }

    public boolean forceAfterEachAppend() {
        return forceAfterEachAppend;
    }

    public boolean forceAfterRegionFull() {
        return forceAfterRegionFull;
    }
}
