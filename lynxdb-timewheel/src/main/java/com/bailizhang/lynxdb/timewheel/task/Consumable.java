package com.bailizhang.lynxdb.timewheel.task;

@FunctionalInterface
public interface Consumable {
    void consume(byte[] data);
}
