package com.bailizhang.lynxdb.timewheel;

@FunctionalInterface
public interface TaskConsumer {
    void consume(byte[] data);
}
