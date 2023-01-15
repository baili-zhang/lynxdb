package com.bailizhang.lynxdb.timewheel.task;

@FunctionalInterface
public interface TaskConsumer {
    void consume(byte[] data);
}
