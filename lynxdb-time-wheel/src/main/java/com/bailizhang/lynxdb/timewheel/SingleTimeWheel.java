package com.bailizhang.lynxdb.timewheel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleTimeWheel {
    private final List<TimeoutTask>[] tasks;
    private final AtomicInteger current = new AtomicInteger(0);

    private volatile boolean nextRound = false;

    @SuppressWarnings("unchecked")
    public SingleTimeWheel(int slotSize) {
        tasks = new List[slotSize];

        for(int i = 0; i < tasks.length; i ++) {
            tasks[i] = new ArrayList<>();
        }
    }

    public void register(int slotNo, TimeoutTask task) {
        tasks[slotNo].add(task);
    }

    public void register(long currentTime, List<TimeoutTask> taskList) {

    }

    public boolean isNextRound() {
        boolean isNextRound = nextRound;
        nextRound = false;
        return isNextRound;
    }

    public List<TimeoutTask> nextTick() {
        int currentSlot = current.getAndIncrement();

        List<TimeoutTask> taskList = tasks[currentSlot];
        tasks[currentSlot] = new ArrayList<>();

        if(current.get() >= tasks.length) {
            nextRound = true;
            current.set(0);
        }

        return taskList;
    }
}
