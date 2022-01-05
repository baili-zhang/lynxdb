package zbl.moonlight.server.executor;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class ExecutorTest {

    /* 一个简单的执行器，将输入的数组加一 */
    class SimpleExecutor extends Executor<Integer> {

        protected SimpleExecutor(Executable<Integer> downStreamExecutor, Thread downStreamThread) {
            super(downStreamExecutor, downStreamThread);
        }

        @Override
        public void run() {

        }
    }

    @Test
    void run() {
    }

    void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}