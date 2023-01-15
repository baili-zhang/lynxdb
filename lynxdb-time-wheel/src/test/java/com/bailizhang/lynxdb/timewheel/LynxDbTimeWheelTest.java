package com.bailizhang.lynxdb.timewheel;

import com.bailizhang.lynxdb.core.utils.TimeUtils;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class LynxDbTimeWheelTest {
    private LynxDbTimeWheel lynxDbTimeWheel;

    @BeforeEach
    void setUp() {
        lynxDbTimeWheel = new LynxDbTimeWheel();
        new Thread(lynxDbTimeWheel).start();
        TimeUtils.sleep(TimeUnit.SECONDS, 1);
    }

    @Test
    void test() throws InterruptedException {
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
}