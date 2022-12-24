package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.executor.Shutdown;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class LynxDbTimeWheel extends Shutdown implements Runnable {
    private static final int INTERVAL_MILLS = 5;

    private static final int SECOND_TIME_WHEEL_SLOT_SIZE = 200;
    private static final int MINUTE_TIME_WHEEL_SLOT_SIZE = 60;
    private static final int HOUR_TIME_WHEEL_SLOT_SIZE = 60;
    private static final int TOP_TIME_WHEEL_SLOT_SIZE = 10;

    private static final int SECOND = 1000;
    private static final int MINUTE = 60 * 1000;
    private static final int HOUR = 60 * 60 * 1000;

    private final AtomicLong id = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TimeoutTask> tasks = new ConcurrentHashMap<>();

    private final SingleTimeWheel secondTimeWheel = new SingleTimeWheel(SECOND_TIME_WHEEL_SLOT_SIZE);
    private final SingleTimeWheel minuteTimeWheel = new SingleTimeWheel(MINUTE_TIME_WHEEL_SLOT_SIZE);
    private final SingleTimeWheel hourTimeWheel = new SingleTimeWheel(HOUR_TIME_WHEEL_SLOT_SIZE);
    private final PriorityTimeWheel topTimeWheel = new PriorityTimeWheel(TOP_TIME_WHEEL_SLOT_SIZE);

    private final TaskConsumer consumer;

    private long currentTime;

    public LynxDbTimeWheel(TaskConsumer taskConsumer) {
        consumer = taskConsumer;
    }

    // 需要保证线程安全
    public long register(TimeoutTask task) {
        long taskId = id.getAndIncrement();
        tasks.put(taskId, task);

        long duration = task.time() - System.currentTimeMillis();
        if(duration < INTERVAL_MILLS) {
            task.run();
        }

        if(duration < SECOND) {
            int slot = ((int) duration) % INTERVAL_MILLS;
            secondTimeWheel.register(slot, task);
        } else if(duration < MINUTE) {
            int slot = ((int) duration) % SECOND;
            minuteTimeWheel.register(slot, task);
        } else if(duration < HOUR) {
            int slot = ((int) duration) % MINUTE;
            hourTimeWheel.register(slot, task);
        } else {
            int slot = ((int) duration) % HOUR;
            topTimeWheel.register(slot, task);
        }

        return taskId;
    }

    // 需要保证线程安全
    public void unregister(long id) {
        tasks.remove(id);
    }

    @Override
    public void run() {
        currentTime = System.currentTimeMillis();

        while (isNotShutdown()) {
            List<TimeoutTask> taskList = secondTimeWheel.nextTick();
            if(secondTimeWheel.isNextRound()) {

            }

            for(TimeoutTask task : taskList) {
                consumer.consume(task.data());
            }
        }
    }
}
