package com.bailizhang.lynxdb.timewheel.types;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.TimeUtils;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class TimeWheelTest {

    private TimeWheel timeWheel;
    private volatile boolean shutdown = false;
    private volatile boolean initialized = false;

    @BeforeEach
    void setUp() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));

        new Thread(() -> {
            TimeWheel topTimeWheel = new TopperTimeWheel(10, 500);
            timeWheel = new LowerTimeWheel(5, 50, topTimeWheel);

            long beginTime = (System.currentTimeMillis() / 10) * 10;
            timeWheel.init(beginTime);

            long nextTime = beginTime + 10;

            initialized = true;

            while (!shutdown) {
                // 线程睡眠到被调度消耗时间，所以精度不可能太高
                while (System.currentTimeMillis() < nextTime) {
                    TimeUtils.sleep(TimeUnit.MILLISECONDS, 1);
                }

                List<TimeoutTask> tasks = timeWheel.tick();
                tasks.forEach(TimeoutTask::doTask);

                nextTime += 10;
            }
        }).start();
    }

    @Test
    void testRegister() throws InterruptedException {
        while (!initialized) {
            TimeUtils.sleep(TimeUnit.SECONDS, 1);
        }

        long current = System.currentTimeMillis();

        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);

        for(int i = 0; i < taskCount; i ++) {
            TimeoutTask task = new TimeoutTask(current + 3000 * (i + 1), () -> {
                System.out.println(System.currentTimeMillis());
                latch.countDown();
            });

            timeWheel.register(task);
        }

        latch.await();

        shutdown = true;
    }

    @Test
    void testUnregister() throws InterruptedException {
        while (!initialized) {
            TimeUtils.sleep(TimeUnit.SECONDS, 1);
        }

        long current = System.currentTimeMillis();

        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);

        Runnable runnable = () -> {
            System.out.println(System.currentTimeMillis());
            latch.countDown();
        };

        String identifier = "task";

        for(int i = 0; i < taskCount; i ++) {

            TimeoutTask task = new TimeoutTask(
                    current + 3000 * (i + 1),
                    G.I.toBytes(identifier + i),
                    runnable
            );

            timeWheel.register(task);
        }

        int removeIndex = 2;
        TimeoutTask task = new TimeoutTask(
                current + 3000 * (removeIndex + 1),
                G.I.toBytes(identifier + removeIndex),
                runnable
        );

        timeWheel.unregister(task);
        latch.countDown();

        latch.await();

        shutdown = true;
    }
}