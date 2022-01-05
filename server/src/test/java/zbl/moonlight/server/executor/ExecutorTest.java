package zbl.moonlight.server.executor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExecutorTest {

    /* 一个简单的执行器，将输入的数组加一 */
    class SimpleExecutor extends Executor<Integer, Integer> {
        public SimpleExecutor(Thread notifiedThread) {
            super(notifiedThread);
        }

        @Override
        public void run() {
            while(true) {
                Integer in = pollIn();
                if(in != null) {
                    offerOut(in + 1);
                    break;
                }
            }
        }
    }

    @Test
    void run() {
        Executor<Integer, Integer> executor = new SimpleExecutor(Thread.currentThread());
        Thread t1 = new Thread(executor);
        t1.start();
        executor.offer(1);

        if (Thread.State.TIMED_WAITING.equals(t1.getState())) {
            t1.interrupt();
        }

        Integer result = executor.poll();
        while (result == null) {
            result = executor.poll();
            sleep();
        }
        assert result.equals(2);
    }

    void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}