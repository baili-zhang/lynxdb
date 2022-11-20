package com.bailizhang.lynxdb.timewheel;

import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PriorityTimeWheel {
    private final PriorityQueue<TimeoutTask>[] tasks;

    private AtomicInteger current = new AtomicInteger(0);

    @SuppressWarnings("unchecked")
    public PriorityTimeWheel(int slotSize) {
        tasks = new PriorityQueue[slotSize];

        for(int i = 0; i < tasks.length; i ++) {
            tasks[i] = new PriorityQueue<>();
        }
    }

    public void register(int slotNo, TimeoutTask task) {
        tasks[slotNo + current.get()].add(task);
    }
}
