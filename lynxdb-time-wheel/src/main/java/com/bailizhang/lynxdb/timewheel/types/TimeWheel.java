package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.util.ArrayList;
import java.util.List;

public abstract class TimeWheel {
    public static final int SUCCESS = -1;

    /** 时间轮 */
    protected final List<TimeoutTask>[] circle;
    /** 时间轮的刻度值大小 */
    protected final int scale;
    /** 当前的刻度值 */
    protected int slot = 0;
    /** 时间轮的总时间 */
    protected final int totalTime;
    /** 每个时间刻度的时间 */
    protected final int millisPerSlot;
    /** 时间轮刻度 0 的时间 */
    protected long baseTime;

    @SuppressWarnings("unchecked")
    public TimeWheel(int scale, int totalTime) {
        this.scale = scale;
        this.totalTime = totalTime;

        millisPerSlot = totalTime / scale;
        circle = new List[scale];

        for(int i = 0; i < scale; i ++) {
            circle[i] = new ArrayList<>();
        }
    }

    public List<TimeoutTask> tick() {
        if(++ slot >= scale) {
            List<TimeoutTask> nextRound = nextRound();

            baseTime += totalTime;

            nextRound.forEach(task -> {
                long time = task.time();
                int delta = (int)(time - baseTime);

                // 理论上不会出现，只是保证逻辑的完整性
                if(delta < 0) {
                    throw new RuntimeException();
                }

                int slot = delta / millisPerSlot;
                circle[slot].add(task);
            });

            // 更新 slot
            slot = 0;
        }

        List<TimeoutTask> tasks = circle[slot];
        circle[slot] = new ArrayList<>();

        return tasks;
    }

    public abstract int init(long time);
    public abstract int register(TimeoutTask task);
    protected abstract List<TimeoutTask> nextRound();
}
