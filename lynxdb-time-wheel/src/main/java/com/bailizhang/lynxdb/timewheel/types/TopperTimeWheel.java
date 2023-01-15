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
    protected void registerNext(TimeoutTask task) {
        queue.add(task);
    }

    @Override
    protected List<TimeoutTask> nextRound() {
        List<TimeoutTask> tasks = new ArrayList<>();

        while(!queue.isEmpty()) {
            TimeoutTask task = queue.peek();

            if(task.time() >= baseTime + totalTime) {
                break;
            }

            tasks.add(task);
        }

        return tasks;
    }

    @Override
    protected void initNextTimeWheel(int delta, long base) {
        // Do nothing
    }
}
