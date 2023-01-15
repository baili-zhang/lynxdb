package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.core.utils.TimeUtils;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import com.bailizhang.lynxdb.timewheel.types.LowerTimeWheel;
import com.bailizhang.lynxdb.timewheel.types.TimeWheel;
import com.bailizhang.lynxdb.timewheel.types.TopperTimeWheel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class LynxDbTimeWheel extends Shutdown implements Runnable {
    private static final Logger logger = LogManager.getLogger("LynxDbTimeWheel");

    private static final int INTERVAL_MILLS = 10;
    private static final int HOURS_PER_DAY = 24;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int SECONDS_PER_MINUTE = 60;
    private static final int TEN_MILLIS_PER_SECOND = 100;
    private static final int TOTAL_COUNT = HOURS_PER_DAY * MINUTES_PER_HOUR * SECONDS_PER_MINUTE * TEN_MILLIS_PER_SECOND;

    /** 秒钟的时间轮 */
    private final TimeWheel second;

    public LynxDbTimeWheel() {
        int secondMillis = TEN_MILLIS_PER_SECOND * INTERVAL_MILLS;
        int minuteMillis = SECONDS_PER_MINUTE * secondMillis;
        int hourMillis = MINUTES_PER_HOUR * minuteMillis;
        int dayMillis = HOURS_PER_DAY * hourMillis;

        TimeWheel day = new TopperTimeWheel(HOURS_PER_DAY, dayMillis);
        TimeWheel hour = new LowerTimeWheel(MINUTES_PER_HOUR, hourMillis, day);
        TimeWheel minute = new LowerTimeWheel(SECONDS_PER_MINUTE, minuteMillis, hour);

        second = new LowerTimeWheel(TEN_MILLIS_PER_SECOND, secondMillis, minute);
    }

    @Override
    public void run() {
        long beginTime = (System.currentTimeMillis() / TEN_MILLIS_PER_SECOND) * TEN_MILLIS_PER_SECOND;

        int delta = (int)(beginTime % TOTAL_COUNT);
        long base = beginTime - delta;

        second.init(delta, base);

        long nextTime = beginTime + INTERVAL_MILLS;

        while (isNotShutdown()) {
            // 线程睡眠到被调度消耗时间，所以精度不可能太高
            while (System.currentTimeMillis() < nextTime) {
                TimeUtils.sleep(TimeUnit.MILLISECONDS, 1);
            }

            List<TimeoutTask> tasks = second.tick();
            tasks.forEach(TimeoutTask::run);

            nextTime += INTERVAL_MILLS;

            logger.info("Tick {}", nextTime);
        }
    }
}
