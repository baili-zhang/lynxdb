package com.bailizhang.lynxdb.core.log;

public class LogGroupOptions {
    private Integer regionCapacity;
    private Integer regionBlockSize;
    private boolean force = false;

    public LogGroupOptions() {
        this.regionCapacity = null;
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

    public int regionCapacity() {
        if(regionCapacity == null) {
            throw new RuntimeException();
        }

        return regionCapacity;
    }

    public void force(boolean val) {
        force = val;
    }

    public boolean isForce() {
        return force;
    }

    public void regionBlockSize(int size) {
        if(regionBlockSize == null) {
            regionBlockSize = size;
        }
    }

    public int regionBlockSizeOrDefault(int defaultValue) {
        return regionBlockSize == null ? defaultValue : regionBlockSize;
    }
}
