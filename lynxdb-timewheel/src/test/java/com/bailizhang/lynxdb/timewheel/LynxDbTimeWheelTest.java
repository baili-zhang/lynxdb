package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.TimeUtils;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class LynxDbTimeWheelTest {
    private LynxDbTimeWheel lynxDbTimeWheel;

    @BeforeEach
    void setUp() {
        lynxDbTimeWheel = new LynxDbTimeWheel();
        new Thread(lynxDbTimeWheel).start();
        TimeUtils.sleep(TimeUnit.SECONDS, 1);
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    @Test
    void testRegister() throws InterruptedException {
        long current = System.currentTimeMillis();

        int taskCount = 10;
        CountDownLatch latch = new CountDownLatch(taskCount);

        for(int i = 0; i < taskCount; i ++) {
            TimeoutTask task = new TimeoutTask(current + 3000 * (i + 1), () -> {
                System.out.println(System.currentTimeMillis());
                latch.countDown();
            });

            lynxDbTimeWheel.register(task);
        }

        latch.await();
        lynxDbTimeWheel.shutdown();
    }

    @Test
    void testUnregister() throws InterruptedException {
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

            lynxDbTimeWheel.register(task);
        }

        int removeIndex = 2;
        TimeoutTask task = new TimeoutTask(
                current + 3000 * (removeIndex + 1),
                G.I.toBytes(identifier + removeIndex),
                runnable
        );

        lynxDbTimeWheel.unregister(task);
        latch.countDown();

        latch.await();
        lynxDbTimeWheel.shutdown();
    }
}