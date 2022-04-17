package zbl.moonlight.server.raft;

import lombok.Getter;
import lombok.Setter;
import zbl.moonlight.server.raft.log.RaftLog;
import zbl.moonlight.server.raft.log.RaftLogEntry;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Setter
@Getter
public class RaftState {
    /**
     * 默认心跳的时间间隔（毫秒数）
     */
    public final static int HEARTBEAT_INTERVAL_MILLIS = 3000;
    /**
     * 选举的最小超时时间间隔
     */
    private final static int TIMEOUT_MIN_MILLIS = 10000;
    /**
     * 选举的最大超时时间
     */
    private static final int TIMEOUT_MAX_MILLIS = 15000;
    /**
     * 选举的超时时间
     */
    public static final int TIMEOUT_MILLIS = TIMEOUT_MIN_MILLIS
            + (int)((TIMEOUT_MAX_MILLIS - TIMEOUT_MIN_MILLIS) * Math.random());
    /**
     * 心跳定时器
     */
    private volatile long heartbeatTimeMillis = System.currentTimeMillis();
    /**
     * 超时计时器
     */
    private volatile long timeoutTimeMillis = System.currentTimeMillis();
    /**
     * 当前节点的角色
     */
    private volatile RaftRole raftRole = RaftRole.Follower;

    /**
     * 当前获取到的投票数
     */
    private final HashSet<RaftNode> votedNodes = new HashSet<>();
    /**
     * 不包含当前节点的其他节点
     */
    private final List<RaftNode> raftNodes;

    /* -------------- 持久化数据 -------------- */
    /**
     * 任期号
     */
    private volatile int currentTerm;
    /**
     * 投票给了哪个节点
     */
    private volatile RaftNode votedFor;
    /**
     * 需要同步的日志
     */
    private final RaftLog raftLog;

    /* ---------- 易失的状态（所有服务器） ------- */
    private volatile int commitIndex;

    public int lastApplied() {
        return raftLog.getCursor() - 1;
    }

    /* ----------- 易失的状态（Leader） -------- */
    private final ConcurrentHashMap<RaftNode, Integer> nextIndex;
    private final ConcurrentHashMap<RaftNode, Integer> matchIndex;

    public RaftState (List<RaftNode> nodes, String host, int port) throws IOException {
        raftNodes = nodes;

        nextIndex = new ConcurrentHashMap<>();
        matchIndex = new ConcurrentHashMap<>();

        /* 初始化各个节点的日志索引 */
        for (RaftNode node : nodes) {
            nextIndex.put(node, 0);
            matchIndex.put(node, 0);
        }

        String fileNamePrefix = host + "_" + port + "_";
        /* 数据文件名 */
        String dataFileName = fileNamePrefix + "data";
        /* 索引文件名 */
        String indexFileName = fileNamePrefix + "index";
        /* 验证文件名 */
        String verifyFileName = fileNamePrefix + "verify";

        raftLog = new RaftLog(dataFileName, indexFileName, verifyFileName);
    }

    public int lastLogTerm() throws IOException {
        if(lastApplied() == -1) {
            return 0;
        }
        RaftLogEntry logEntry = raftLog.read(lastApplied());
        return logEntry.term();
    }
}
