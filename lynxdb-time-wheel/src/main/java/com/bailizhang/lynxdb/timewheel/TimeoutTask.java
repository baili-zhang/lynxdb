package com.bailizhang.lynxdb.timewheel;

public abstract class TimeoutTask implements Runnable {
    private final long time;
    private final byte[] data;

    public TimeoutTask(long time, byte[] data) {
        this.time = time;
        this.data = data;
    }

    public TimeoutTask(long time) {
        this(time, null);
    }

    public long time() {
        return time;
    }

    public byte[] data() {
        return data;
    }
}
