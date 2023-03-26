package com.bailizhang.lynxdb.core.log;

public class LogGroupOptions {
    private final int extraDataLength;
    private Integer regionCapacity;
    private boolean force = false;

    public LogGroupOptions(int extraDataLength) {
        this.extraDataLength = extraDataLength;
        this.regionCapacity = null;
    }

    public int extraDataLength() {
        return extraDataLength;
    }

    public void regionCapacity(int val) {
        if(val <= 0 || regionCapacity != null) {
            return;
        }

        regionCapacity = val;
    }

    public Integer regionCapacityOrDefault(int defaultValue) {
        return regionCapacity == null ? defaultValue : regionCapacity;
    }

    public void force(boolean val) {
        force = val;
    }

    public boolean isForce() {
        return force;
    }
}
