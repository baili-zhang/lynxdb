package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.executor.Shutdown;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class LynxDbTimeWheel extends Shutdown implements Runnable {
    private static final Logger logger = LogManager.getLogger("LynxDbTimeWheel");

    private static final int INTERVAL_MILLS = 10;
    private static final int DAY_COUNT = 24;
    private static final int HOUR_COUNT = 60;
    private static final int MINUTE_COUNT = 60;
    private static final int SECOND_COUNT = 100;
    private static final int TOTAL_COUNT = DAY_COUNT * HOUR_COUNT * MINUTE_COUNT * SECOND_COUNT;

    private final AtomicLong id = new AtomicLong(1);
    private final ConcurrentHashMap<Long, TimeoutTask> tasks = new ConcurrentHashMap<>();

    /** 秒钟的时间轮 */
    private final TimeWheel second;

    private final TaskConsumer consumer;

    public LynxDbTimeWheel(TaskConsumer taskConsumer) {
        consumer = taskConsumer;

        TimeWheel day = new TimeWheel(DAY_COUNT, null);
        TimeWheel hour = new TimeWheel(HOUR_COUNT, day);
        TimeWheel minute = new TimeWheel(MINUTE_COUNT, hour);

        second = new TimeWheel(SECOND_COUNT, minute);
    }

    public synchronized long register(TimeoutTask task) {
        long taskId = id.getAndIncrement();
        tasks.put(taskId, task);

        long duration = task.time() - System.currentTimeMillis();
        if(duration < INTERVAL_MILLS) {
            task.run();
        }

        return taskId;
    }

    // 需要保证线程安全
    public void unregister(long id) {
        tasks.remove(id);
    }

    @Override
    public void run() {
        long current = System.currentTimeMillis();
        int delta = (int) current % TOTAL_COUNT;
        second.init(delta);

        long nextTime = current + INTERVAL_MILLS;

        while (isNotShutdown()) {
            // 线程睡眠到被调度消耗时间，所以精度不可能太高
            while(System.currentTimeMillis() < nextTime) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            second.tick();

            nextTime += INTERVAL_MILLS;

            logger.info("Tick");
        }
    }
}
