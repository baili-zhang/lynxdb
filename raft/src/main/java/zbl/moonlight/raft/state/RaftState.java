package zbl.moonlight.raft.state;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.timeout.Timeout;
import zbl.moonlight.core.timeout.TimeoutTask;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.log.RaftLog;
import zbl.moonlight.raft.log.TermLog;
import zbl.moonlight.raft.request.AppendEntries;
import zbl.moonlight.raft.request.RequestVote;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RaftState：单例，需要保证线程安全
 */
public class RaftState {
    private static final Logger logger = LogManager.getLogger("RaftState");

    private static final RaftState RAFT_STATE;

    private static final String LOG_FILENAME_PREFIX = "raft_";
    private static final String HEARTBEAT_TIMEOUT_NAME = "HeartBeat_Timeout";
    private static final String ELECTION_TIMEOUT_NAME = "Election_Timeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;

    static {
        // 通过 SPI 获取 StateMachine 的实例
        ServiceLoader<StateMachine> stateMachines = ServiceLoader.load(StateMachine.class);

        Optional<StateMachine> stateMachine = stateMachines.findFirst();

        if(stateMachine.isEmpty()) {
            throw new RuntimeException("Can not find StateMachine.");
        }

        // 通过 SPI 获取 RaftConfiguration 的实例
        ServiceLoader<RaftConfiguration> raftConfigurations = ServiceLoader.load(RaftConfiguration.class);

        Optional<RaftConfiguration> raftConfiguration = raftConfigurations.findFirst();

        if(raftConfiguration.isEmpty()) {
            throw new RuntimeException("Can not find RaftConfiguration.");
        }

        try {
            RAFT_STATE = new RaftState(stateMachine.get(), raftConfiguration.get(), LOG_FILENAME_PREFIX);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final Timeout heartbeat;
    private final Timeout election;

    private RaftClient raftClient;

    public RaftClient raftClient() {
        if(raftClient == null) {
            throw new RuntimeException("[raftClient] is null.");
        }
        return raftClient;
    }

    public void raftClient(RaftClient client) {
        if(raftClient == null) {
            raftClient = client;
        }
    }

    public static RaftState getInstance() {
        return RAFT_STATE;
    }

    private final ServerNode currentNode;

    private RaftState(StateMachine stateMachine, RaftConfiguration raftConfiguration, String logFilenamePrefix)
            throws IOException {
        this.stateMachine = stateMachine;
        this.raftConfiguration = raftConfiguration;

        currentNode = raftConfiguration.currentNode();

        heartbeat = new Timeout(new HeartbeatTask(), HEARTBEAT_INTERVAL_MILLIS);
        /* 设置随机选举超时时间 */
        final int ELECTION_INTERVAL_MILLIS = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;
        election = new Timeout(new ElectionTask(), ELECTION_INTERVAL_MILLIS);

        raftLog = new RaftLog(logFilenamePrefix + "index.log",
                logFilenamePrefix + "data.log");
        termLog = new TermLog(logFilenamePrefix + "term.log");
    }

    private final RaftConfiguration raftConfiguration;

    public void startTimeout() {
        // 如果 leaderMode 为 follower，不需要启动超时计时器
        if(raftConfiguration.leaderNode() == RaftConfiguration.FOLLOWER) {
            return;
        }
        // 启动心跳超时计时器
        new Thread(heartbeat, HEARTBEAT_TIMEOUT_NAME).start();
        // 启动选举超时计时器
        new Thread(election, ELECTION_TIMEOUT_NAME).start();
    }

    public ServerNode currentNode() {
        return currentNode;
    }

    private class HeartbeatTask implements TimeoutTask {
        private static final Logger logger = LogManager.getLogger("HeartbeatTask");

        @Override
        public void run() {
            try {
                /* 如果心跳超时，则需要发送心跳包 */
                if (raftRole() == RaftRole.Leader) {
                    for (SelectionKey selectionKey : raftClient.connectedNodes()) {
                        int prevLogIndex = nextIndex().get(selectionKey) - 1;
                        int leaderCommit = commitIndex();

                        int prevLogTerm = prevLogIndex == 0 ? 0
                                : getEntryTermByIndex(prevLogIndex);
                        Entry[] entries = getEntriesByRange(prevLogIndex,
                                indexOfLastLogEntry());

                        AppendEntries appendEntries = new AppendEntries();

                        if(raftClient.isConnected(selectionKey)) {
                            raftClient.sendMessage(selectionKey, appendEntries);

                            logger.debug("[{}] send {} to node: {}.", currentNode,
                                    appendEntries, ((SocketChannel)selectionKey.channel()).getRemoteAddress());
                        }
                    }
                }

                /* 心跳超时会中断 socketClient，用来 connect 其他节点 */
                raftClient.interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ElectionTask implements TimeoutTask {
        private static final Logger logger = LogManager.getLogger("ElectionTask");

        @Override
        public void run() {
            try {
                /* 如果选举超时，需要转换为 Candidate，则向其他节点发送 RequestVote 请求 */
                if (raftRole() != RaftRole.Leader) {
                    if(raftConfiguration.leaderNode() == RaftConfiguration.CANDIDATE) {
                        // 配置中的总节点数
                        int count = stateMachine.clusterNodes().size();
                        // 连接上的节点数
                        int connect = nextIndex.size();
                        // 如果连接上的节点数超过配置中的总节点数的半数
                        if(connect > (count >> 1)) {
                            // 转换为 Candidate 角色
                            transformToCandidate();
                        }
                    } else if(raftConfiguration.leaderNode() == RaftConfiguration.LEADER) {
                        // 转换为 Candidate 角色
                        transformToCandidate();
                    }

                    Entry lastEntry = lastEntry();
                    int term = lastEntry == null ? 0 : lastEntry.term();

                    RequestVote requestVote = new RequestVote();
                    requestVote.term(currentTerm());
                    requestVote.candidate(currentNode);
                    requestVote.lastLogIndex(indexOfLastLogEntry());
                    requestVote.lastLogTerm(term);

                    logger.info("[{}] -- [{}] -- Election timeout, " +
                                    "Send RequestVote [{}] to other nodes.",
                            currentNode, raftRole(), requestVote);

                    raftClient.broadcastMessage(requestVote);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private final HashSet<SelectionKey> votedNodes = new HashSet<>();
    public void setVotedNodeAndCheck(SelectionKey selectionKey) throws IOException {
        if(raftRole == RaftRole.Leader) {
            return;
        }
        synchronized (this) {
            votedNodes.add(selectionKey);
//            List<ServerNode> allNodes = stateMachine.clusterNodes();
//            if(votedNodes.size() > (allNodes.size() >> 1)) {
//                raftRole = RaftRole.Leader;
//                /* 获取所有的 follower 节点 */
//                List<SelectionKey> followers = allNodes.stream()
//                        .filter((node) -> !node.equals(currentNode)).toList();
//                nextIndex.clear();
//                matchedIndex.clear();
//                /* 初始化 leader 的相关属性 */
//                int lastEntryIndex = indexOfLastLogEntry();
//                for (SelectionKey selectionKey : followers) {
//                    nextIndex.put(selectionKey, lastEntryIndex + 1);
//                    matchedIndex.put(selectionKey, 0);
//                }
//            }
        }
    }

    private volatile ServerNode leaderNode;
    public ServerNode leaderNode() {
        return leaderNode;
    }

    public void setLeaderNode(ServerNode leaderNode) {
        this.leaderNode = leaderNode;
    }

    public void setRaftRole(RaftRole raftRole) {
        this.raftRole = raftRole;
    }

    private volatile RaftRole raftRole = RaftRole.Follower;
    public RaftRole raftRole() {
        return raftRole;
    }
    public synchronized void transformToCandidate() throws IOException {
        setRaftRole(RaftRole.Candidate);
        setCurrentTerm(currentTerm() + 1);
//        setVoteFor(currentNode);
//        votedNodes.add(currentNode);
    }

    /**
     * Raft 日志
     */
    private final RaftLog raftLog;

    /**
     * 返回 raft 日志的最后一个条目
     * @return 最后一个条目
     * @throws IOException IO异常
     */
    public Entry lastEntry() throws IOException {
        return raftLog.lastEntry();
    }

    /**
     * 通过索引值获取该索引值处的日志条目
     * @param index 索引值
     * @return 日志条目
     * @throws IOException IO异常
     */
    public Entry getEntryByIndex(int index) throws IOException {
        return raftLog.getEntryByIndex(index);
    }

    public int getEntryTermByIndex(int index) throws IOException {
        return raftLog.getEntryTermByIndex(index);
    }

    /**
     * 设置最大的有效日志索引值
     * @param index 索引值
     * @throws IOException IO异常
     */
    public void setMaxIndex(int index) throws IOException {
        raftLog.setMaxIndex(index);
    }

    /**
     * 将日志条目添加到日志的尾部
     * @param entry 日志条目
     * @throws IOException IO异常
     */
    public int append(Entry entry) throws IOException {
        return raftLog.append(entry);
    }
    public void append(Entry[] entries) throws IOException {
        raftLog.append(entries);
    }
    public Entry[] getEntriesByRange(int begin, int end) throws IOException {
        return raftLog.getEntriesByRange(begin, end);
    }

    /**
     * 返回最后一个 logEntry 的索引值
     * @return 索引值
     */
    public int indexOfLastLogEntry() throws IOException {
        return raftLog.indexOfLastLogEntry();
    }

    /**
     * 用来记录当前任期和投票给的节点
     */
    private final TermLog termLog;

    /**
     * 返回当前的任期号
     * @return 当前任期号
     */
    public int currentTerm() throws IOException {
        return termLog.currentTerm();
    }

    /**
     * 设置当前任期号
     * @param term 任期号
     */
    public void setCurrentTerm(int term) throws IOException {
        termLog.setCurrentTerm(term);
    }

    /**
     * 获取投票给的节点
     * @return 投票给的节点
     */
    public ServerNode voteFor() throws IOException {
        return termLog.voteFor();
    }

    public synchronized void setVoteFor(ServerNode node)
            throws IOException {
        termLog.setVoteFor(node);
    }

    /**
     * 已提交的日志索引
     */
    private final AtomicInteger commitIndex = new AtomicInteger(0);

    /**
     * 应用到状态机的日志索引
     */
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    public int commitIndex() {
        return commitIndex.get();
    }
    public void setCommitIndex(int index) {
        commitIndex.set(index);
    }
    public int lastApplied() {
        return lastApplied.get();
    }

    private final ConcurrentHashMap<SelectionKey, Integer> nextIndex
            = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SelectionKey, Integer> matchedIndex
            = new ConcurrentHashMap<>();
    public ConcurrentHashMap<SelectionKey, Integer> nextIndex() {
        return nextIndex;
    }
    public ConcurrentHashMap<SelectionKey, Integer> matchedIndex() {
        return matchedIndex;
    }

    /**
     * 检查 commitIndex，如果可以增加，则增加 commitIndex
     */
    public void checkCommitIndex() throws IOException {

        int n = (stateMachine.clusterNodes().size() >> 1) + 1;
        int maxIndex = indexOfLastLogEntry();
        for(int i = commitIndex.get() + 1; i <= maxIndex; i ++) {
            int count  = 1;
            for(SelectionKey selectionKey : matchedIndex.keySet()) {
                if(matchedIndex.get(selectionKey) >= i) {
                    count ++;
                }
            }
            if(count >= n && i > commitIndex.get()) {
                int oldCommitIndex = commitIndex.get();
                commitIndex.set(i);
            }
        }
        /* 将提交的日志应用到状态机 */
        if(commitIndex() > lastApplied()) {
            apply(getEntriesByRange(lastApplied(), commitIndex()));
        }
    }

    private final StateMachine stateMachine;

    /**
     * 将日志条目应用到状态机
     * @param entries 日志条目
     */
    public void apply(Entry[] entries) {
        stateMachine.apply(entries);
        lastApplied.set(commitIndex.get());
    }

    public void resetElectionTimeout() {
        election.reset();
    }

    public void resetHeartbeatTimeout() {
        heartbeat.reset();
    }
}
