package zbl.moonlight.server.raft;

import lombok.Getter;
import lombok.Setter;
import zbl.moonlight.server.raft.log.RaftLog;
import zbl.moonlight.server.raft.log.RaftLogEntry;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Setter
@Getter
public class RaftState {
    /** 默认心跳的时间间隔（毫秒数） */
    public final static int HEARTBEAT_INTERVAL_MILLIS = 3000;
    /** 选举的最小超时时间间隔 */
    private final static int TIMEOUT_MIN_MILLIS = 10000;
    /** 选举的最大超时时间 */
    private static final int TIMEOUT_MAX_MILLIS = 15000;
    /** 选举的超时时间 */
    public static int TIMEOUT_MILLIS = TIMEOUT_MIN_MILLIS
            + (int)((TIMEOUT_MAX_MILLIS - TIMEOUT_MIN_MILLIS) * Math.random());

    /** 时间（毫秒数）：用做定时器 */
    private volatile long heartbeatTimeMillis = System.currentTimeMillis();
    private volatile long timeoutTimeMillis = System.currentTimeMillis();
    private volatile RaftRole raftRole = RaftRole.Follower;

    /**
     * 当前获取到的投票数
     */
    private AtomicInteger voteCount = new AtomicInteger(0);

    /* -------------- 持久化数据 -------------- */
    /** 任期号 */
    private volatile int currentTerm;
    /** 投票给了哪个节点 */
    private volatile RaftNode votedFor;
    /** 需要同步的日志 */
    private volatile RaftLog raftLog;

    /* ---------- 易失的状态（所有服务器） ------- */
    private volatile int commitIndex;

    public int lastApplied() {
        return raftLog.getCursor() - 1;
    }

    /* ----------- 易失的状态（Leader） -------- */
    private volatile ConcurrentHashMap<RaftNode, Integer> nextIndex;
    private volatile ConcurrentHashMap<RaftNode, Integer> matchIndex;

    public int lastLogTerm() throws IOException {
        if(lastApplied() == -1) {
            return 0;
        }
        RaftLogEntry logEntry = raftLog.read(lastApplied());
        return logEntry.term();
    }
}
