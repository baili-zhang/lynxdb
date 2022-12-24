package com.bailizhang.lynxdb.timewheel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleTimeWheel {
    private final List<TimeoutTask>[] tasks;
    /** 当前的时间刻度 */
    private final AtomicInteger current = new AtomicInteger(0);
    /** 上一秒的（毫秒，秒，分钟...）值 */
    private long lastTick;

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
        return nextRound;
    }

    public List<TimeoutTask> nextTick() {
        int currentSlot = current.getAndIncrement();

        List<TimeoutTask> taskList = new ArrayList<>();

        for(int i = 0; i <= currentSlot; i ++) {
            if(tasks[currentSlot].isEmpty()) {
                continue;
            }

            taskList.addAll(tasks[currentSlot]);
        }

        // 检查当前时间轮是否遍历完
        if(current.get() >= tasks.length) {
            nextRound = true;
            current.set(0);
        }

        // 阻塞等待下一秒
        long nanoTime = System.nanoTime();

        return taskList;
    }
}
