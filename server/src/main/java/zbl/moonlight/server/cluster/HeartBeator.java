package zbl.moonlight.server.cluster;

import lombok.Getter;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class HeartBeator implements Runnable {
    @Getter
    private final String NAME = "HeartBeator";

    private final DelayQueue<HeartBeatTask> queue = new DelayQueue<>();

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
        queue.offer(new HeartBeatTask(300));
        while (true) {
            try {
                HeartBeatTask task = queue.take();
                /* TODO:发送心跳给各个客户端 */
                System.out.println("send heart beat to other server.");
                queue.offer(new HeartBeatTask(3000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
