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
    public int init(long time) {
        int remain = nextTimeWheel.init(time);
        slot = remain / millisPerSlot;
        baseTime = time - remain;

        return remain % millisPerSlot;
    }

    @Override
    public int register(TimeoutTask task) {
        int remain = nextTimeWheel.register(task);

        // 已经注册成功了
        if(remain < 0) {
            return SUCCESS;
        }

        int newSlot = remain / millisPerSlot;
        if(newSlot > slot) {
            circle[newSlot].add(task);
            return SUCCESS;
        } else if(newSlot < slot) {
            return 0;
        }

        return remain % millisPerSlot;
    }

    @Override
    protected List<TimeoutTask> nextRound() {
        return nextTimeWheel.tick();
    }
}
