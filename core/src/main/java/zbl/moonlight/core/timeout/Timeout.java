package zbl.moonlight.core.timeout;

import java.util.concurrent.TimeUnit;

/**
 * 定时器的实现
 * TODO: 为什么要自己实现定时器？
 */
public class Timeout implements Runnable {
    private static final String DEFAULT_NAME = "Timeout";

    private final TimeoutTask task;
    private final long interval;

    private volatile long time = System.currentTimeMillis();
    private volatile boolean shutdown = false;
    private volatile boolean reset = false;

    public Timeout(TimeoutTask task, long interval) {
        this.task = task;
        this.interval = interval;
    }

    public static void start(Timeout timeout) {
        new Thread(timeout, DEFAULT_NAME).start();
    }

    public void shutdown() {
        shutdown = true;
    }

    public synchronized void reset() {
        time = System.currentTimeMillis();
        reset = true;
    }

    private synchronized void unset() {
        reset = false;
    }

    @Override
    public void run() {
        while (!shutdown) {
            try {
                long sleepTime = interval - (System.currentTimeMillis() - time);
                TimeUnit.MILLISECONDS.sleep(sleepTime);
                /* 同步执行定时任务 */
                if(!reset) {
                    task.run();
                    /* 重置时间 */
                    time = System.currentTimeMillis();
                } else {
                    unset();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
