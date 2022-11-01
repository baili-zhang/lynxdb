package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.executor.Executor;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LynxDbTimeWheel extends Executor<TimeoutTask> implements TimeWheel {
    private final AtomicLong id = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TimeoutTask> tasks = new ConcurrentHashMap<>();

    @Override
    public long register(TimeoutTask task) {
        long taskId = id.getAndIncrement();
        tasks.put(taskId, task);
        return taskId;
    }

    @Override
    public void unregister(long id) {
        tasks.remove(id);
    }

    @Override
    protected void execute() {

    }
}
