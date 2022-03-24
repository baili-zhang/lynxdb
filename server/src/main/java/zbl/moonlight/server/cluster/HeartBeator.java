package zbl.moonlight.server.cluster;

import lombok.Getter;
import zbl.moonlight.server.config.ClusterConfiguration;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.executor.Executable;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

/* 心跳线程：用于发送心跳和Leader选举 */
public class HeartBeator implements Executable {
    /* 默认心跳的时间间隔，为300毫秒 */
    private static final long DEFAULT_TIME_INTERVAL = 300;
    /* 初始的Leader节点 */
    public static final RaftNode DEFAULT_LEADER = null;

    /* 当前节点的角色 */
    private RaftRole raftRole = RaftRole.Follower;
    /* Leader节点 */
    private RaftNode leader = DEFAULT_LEADER;
    /* 延迟队列定时任务 */
    private final DelayQueue<HeartBeatTask> queue = new DelayQueue<>();
    /* 心跳的时间间隔 */
    private final long interval;

    public HeartBeator () {
        this(DEFAULT_TIME_INTERVAL);
    }

    public HeartBeator(long interval) {
        this.interval = interval;
        Configuration config = ServerContext.getInstance().getConfiguration();
        ClusterConfiguration clusterConfig = config.getClusterConfiguration();
    }

    @Override
    public void offer(Event event) {

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
                HeartBeatTask task = queue.take();
                if(leader == DEFAULT_LEADER) {
                    // 发起选举
                    // new RequestVoteRpc(new RaftNode("localhost", 7830))
                    //         .call(new RequestVoteRpc.Arguments(0,0,0,0));
                }
                queue.offer(new HeartBeatTask(interval));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
