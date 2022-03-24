package zbl.moonlight.server.cluster;

import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executable;
import zbl.moonlight.server.executor.Executor;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/* 心跳线程，用于定时向RaftRpcClient执行器offer心跳 */
public class HeartBeator implements Executable {
    /* 默认心跳的时间间隔，为300毫秒 */
    private static final long DEFAULT_TIME_INTERVAL = 300;
    /* 延迟队列定时任务 */
    private final DelayQueue<HeartBeatTask> queue = new DelayQueue<>();
    /* 心跳的时间间隔 */
    private final long interval;
    /* 直接向RaftRpcClient执行器offer心跳，不经过事件总线 */
    private final Executor raftRpcClient;

    public HeartBeator (Executor raftRpcClient) {
        this(raftRpcClient, DEFAULT_TIME_INTERVAL);
    }

    public HeartBeator(Executor raftRpcClient, long interval) {
        this.raftRpcClient = raftRpcClient;
        this.interval = interval;
    }

    @Override
    public void offer(Event event) {
        throw new RuntimeException("HeartBeator executor can not offer event.");
    }

    private static class HeartBeatTask implements Delayed {
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
                queue.take();
                raftRpcClient.offer(new Event(EventType.HEARTBEAT, null));
                queue.offer(new HeartBeatTask(interval));
            } catch (InterruptedException e) {
                /* 清空延迟队列，相当于清空定时器 */
                queue.clear();
                /* 队列尾部加入一个心跳事件，相当于重新设置定时器 */
                queue.offer(new HeartBeatTask(interval));
            }
        }
    }
}
