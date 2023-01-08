package com.bailizhang.lynxdb.timewheel;

public class TimeoutTask implements Runnable {
    private final long time;
    private final byte[] data;
    private final TaskConsumer consumer;

    public TimeoutTask(long time, byte[] data, TaskConsumer consumer) {
        this.time = time;
        this.data = data;
        this.consumer = consumer;
    }

    public TimeoutTask(long time, TaskConsumer consumer) {
        this(time, null, consumer);
    }

    public long time() {
        return time;
    }

    public byte[] data() {
        return data;
    }

    @Override
    public void run() {
        consumer.consume(data);
    }
}
