package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TimeWheel {
    /** 时间轮的刻度值大小 */
    private final int scale;
    /** 时间轮 */
    private final List<TimeoutTask>[] circle;
    /** 当前的刻度值 */
    private final AtomicInteger pointer = new AtomicInteger();

    protected final int totalTime;

    protected long baseTime;

    @SuppressWarnings("unchecked")
    public TimeWheel(int scale, int totalTime) {
        this.scale = scale;
        this.totalTime = totalTime;

        circle = new List[scale];

        for(int i = 0; i < scale; i ++) {
            circle[i] = new ArrayList<>();
        }
    }

    public void init(int delta, long base) {
        int slot = delta % scale;
        pointer.set(slot);

        this.baseTime = base;

        initNextTimeWheel(delta / scale, base);
    }

    public List<TimeoutTask> tick() {
        int slot = pointer.get();
        List<TimeoutTask> tasks = circle[slot];
        circle[slot] = new ArrayList<>();

        slot = slot + 1;
        if(slot >= scale) {
            List<TimeoutTask> nextRound = nextRound();

            nextRound.forEach(this::register);

            baseTime += totalTime;
            pointer.set(0);
        }

        return tasks;
    }

    public void register(TimeoutTask task) {
        long time = task.time();
        int delta = (int) (time - baseTime);

        if(delta >= totalTime) {
            registerNext(task);
            return;
        }

        circle[delta].add(task);
    }

    protected abstract void registerNext(TimeoutTask task);
    protected abstract List<TimeoutTask> nextRound();
    protected abstract void initNextTimeWheel(int delta, long base);
}
