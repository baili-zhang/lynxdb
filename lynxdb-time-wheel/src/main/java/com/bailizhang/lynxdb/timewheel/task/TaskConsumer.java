package com.bailizhang.lynxdb.timewheel.task;

public record TaskConsumer(
        byte[] data,
        Consumable consumable
) implements Runnable {
    @Override
    public void run() {
        consumable.consume(data);
    }
}
