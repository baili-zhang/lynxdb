package zbl.moonlight.client;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

class LynxDbFutureTest {
    private static final byte[] VALUE = "value".getBytes();
    private static final int THREAD_COUNT = 200;

    @Test
    void test_001() throws InterruptedException {
        LynxDbFuture<byte[]> future = new LynxDbFuture<>();
        Thread[] threads = new Thread[THREAD_COUNT];
        CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

        for(int i = 0; i < THREAD_COUNT; i ++) {
            threads[i] = new Thread(() -> {
                latch.countDown();
                byte[] value = future.get();

                assert Arrays.equals(value, VALUE);
            });

            threads[i].start();
        }

        latch.await();

        for(Thread thread : threads) {
            assert thread.getState() == Thread.State.WAITING;
        }

        assert future.blockedThreadCount() == THREAD_COUNT;
        future.value(VALUE);
    }
}