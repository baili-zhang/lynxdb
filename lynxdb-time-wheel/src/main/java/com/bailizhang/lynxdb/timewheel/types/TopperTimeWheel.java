package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

public class TopperTimeWheel extends TimeWheel {

    private final PriorityQueue<TimeoutTask> queue = new PriorityQueue<>();

    public TopperTimeWheel(int scale, int totalTime) {
        super(scale, totalTime);
    }

    @Override
    public int init(long time) {
        int remain = (int)(time % totalTime);
        slot = remain / millisPerSlot;
        baseTime = time - remain;

        return remain % millisPerSlot;
    }

    @Override
    public int register(TimeoutTask task) {
        long time = task.time();

        int remain = (int)(time - baseTime);

        // remain 小于 0，直接返回 0
        if(remain < 0) {
            return 0;
        }

        int newSlot = remain / millisPerSlot;

        // 当前时间轮放不下，放到优先队列里
        if(newSlot >= scale) {
            queue.add(task);
            return SUCCESS;
        } else if(newSlot > slot) {
            // 只能注册到比 slot 大的 slot
            circle[newSlot].add(task);
            return SUCCESS;
        } else if(newSlot < slot) {
            return 0;
        }

        return remain % millisPerSlot;
    }

    @Override
    public int unregister(TimeoutTask task) {
        long time = task.time();

        int remain = (int)(time - baseTime);

        // remain 小于 0，直接返回 0
        if(remain < 0) {
            return 0;
        }

        int newSlot = remain / millisPerSlot;

        // 在优先队列里
        if(newSlot >= scale) {
            queue.remove(task);
            return SUCCESS;
        } else if(newSlot > slot) {
            // 在时间轮里
            circle[newSlot].remove(task);
            return SUCCESS;
        } else if(newSlot < slot) {
            return 0;
        }

        return remain % millisPerSlot;
    }

    @Override
    protected List<TimeoutTask> nextRound() {
        List<TimeoutTask> tasks = new ArrayList<>();

        while(!queue.isEmpty()) {
            TimeoutTask task = queue.peek();

            if(task.time() >= baseTime + totalTime) {
                break;
            }

            tasks.add(queue.poll());
        }

        return tasks;
    }
}
