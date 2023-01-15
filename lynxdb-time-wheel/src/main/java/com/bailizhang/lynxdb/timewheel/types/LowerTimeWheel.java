package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.util.List;

public class LowerTimeWheel extends TimeWheel {
    private final TimeWheel nextTimeWheel;

    public LowerTimeWheel(int scale, int totalTime, TimeWheel next) {
        super(scale, totalTime);
        nextTimeWheel = next;
    }

    @Override
    protected void registerNext(TimeoutTask task) {
        nextTimeWheel.register(task);
    }

    @Override
    protected List<TimeoutTask> nextRound() {
        return nextTimeWheel.tick();
    }

    @Override
    protected void initNextTimeWheel(int delta, long base) {
        nextTimeWheel.init(delta, base);
    }
}
