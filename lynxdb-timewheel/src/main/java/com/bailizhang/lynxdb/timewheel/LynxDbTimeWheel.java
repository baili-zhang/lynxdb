/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.common.CheckThreadSafety;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.core.utils.TimeUtils;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import com.bailizhang.lynxdb.timewheel.types.LowerTimeWheel;
import com.bailizhang.lynxdb.timewheel.types.TimeWheel;
import com.bailizhang.lynxdb.timewheel.types.TopperTimeWheel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class LynxDbTimeWheel extends Shutdown implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(LynxDbTimeWheel.class);

    private static final int INTERVAL_MILLS = 10;
    private static final int HOURS_PER_DAY = 24;
    private static final int MINUTES_PER_HOUR = 60;
    private static final int SECONDS_PER_MINUTE = 60;
    private static final int TEN_MILLIS_PER_SECOND = 100;

    /** 秒钟的时间轮 */
    private final TimeWheel second;

    private volatile boolean initialized = false;

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

    @CheckThreadSafety
    public synchronized void register(TimeoutTask task) {
        if(!initialized) {
            return;
        }

        int remain = second.register(task);

        // 注册成功
        if(remain == TimeWheel.SUCCESS) {
            return;
        }

        // 注册失败，直接当前线程执行
        task.doTask();
    }

    public void unregister(long time, Object identifier) {
        unregister(new TimeoutTask(time, identifier, null));
    }

    @CheckThreadSafety
    public synchronized void unregister(TimeoutTask task) {
        if(!initialized) {
            return;
        }

        if(task == null || task.identifier() == null) {
            return;
        }

        int remain = second.unregister(task);
        if(remain == TimeWheel.SUCCESS) {
            logger.trace("Unregister timeout task success, task: {}", task);
            return;
        }

        logger.trace("Unregister timeout task failed, task: {}", task);
    }

    @CheckThreadSafety
    public synchronized TimeoutTask reset(TimeoutTask task, long resetTime) {
        unregister(task);

        TimeoutTask resetTask = new TimeoutTask(
                resetTime,
                task.identifier(),
                task.runnableTask()
        );

        register(resetTask);
        return resetTask;
    }

    @Override
    public void run() {
        long beginTime = (System.currentTimeMillis() / TEN_MILLIS_PER_SECOND) * TEN_MILLIS_PER_SECOND;
        second.init(beginTime);

        long nextTime = beginTime + INTERVAL_MILLS;

        initialized = true;

        while (isNotShutdown()) {
            // 线程睡眠到被调度消耗时间，所以精度不可能太高
            while (System.currentTimeMillis() < nextTime) {
                TimeUtils.sleep(TimeUnit.MILLISECONDS, 1);
            }

            List<TimeoutTask> tasks;

            // 同步 tick 方法
            synchronized (this) {
                tasks = second.tick();
            }

            tasks.forEach(TimeoutTask::doTask);

            nextTime += INTERVAL_MILLS;
        }
    }
}
