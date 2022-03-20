package zbl.moonlight.server.io;

import lombok.Getter;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class HeartBeator implements Runnable {
    @Getter
    private static final String NAME = "HeartBeator";
    /* 默认心跳的时间间隔，为300毫秒 */
    private static final long DEFAULT_TIME_INTERVAL = 3000;

    private final DelayQueue<HeartBeatTask> queue = new DelayQueue<>();
    /* 心跳的时间间隔 */
    private final long interval;

    public HeartBeator () {
        this(DEFAULT_TIME_INTERVAL);
    }

    public HeartBeator(long interval) {
        this.interval = interval;
    }

    private class HeartBeatTask implements Delayed {
        private final long timeMillis;

        private HeartBeatTask(long delayTimeMillis) {
            this.timeMillis = delayTimeMillis + System.currentTimeMillis();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long delta = timeMillis - System.currentTimeMillis();
            return unit.convert(delta, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if(timeMillis > ((HeartBeatTask) o).timeMillis) {
                return 1;
            } else if (timeMillis < ((HeartBeatTask) o).timeMillis) {
                return -1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public void run() {
        queue.offer(new HeartBeatTask(interval));
        while (true) {
            try {
                HeartBeatTask task = queue.take();
                /* TODO:发送心跳给各个客户端 */
                // System.out.println("send heart beat to other server.");
                queue.offer(new HeartBeatTask(interval));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
