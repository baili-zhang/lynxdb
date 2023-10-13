package com.bailizhang.lynxdb.core.log;

public class LogGroupOptions {
    private Integer regionCapacity;
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

    public void force(boolean val) {
        force = val;
    }

    public boolean isForce() {
        return force;
    }
}
