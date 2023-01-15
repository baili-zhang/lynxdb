package com.bailizhang.lynxdb.timewheel.task;

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

    @Override
    public void run() {
        consumer.consume(data);
    }
}
