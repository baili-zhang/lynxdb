package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.file.LogFile;
import com.bailizhang.lynxdb.core.file.LogFileGroup;
import com.bailizhang.lynxdb.raft.timeout.ElectionTask;
import com.bailizhang.lynxdb.raft.timeout.HeartbeatTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.timeout.Timeout;
import com.bailizhang.lynxdb.core.timeout.TimeoutTask;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.RequestVote;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.request.WritableSocketRequest;

import java.io.IOException;
import java.lang.module.Configuration;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RaftState：单例，需要保证线程安全
 */
public class RaftState {
    private static final Logger logger = LogManager.getLogger("RaftState");

    public final static byte DATA_CHANGE = (byte) 0x01;
    public final static byte CLUSTER_MEMBERSHIP_CHANGE = (byte) 0x02;

    private static final RaftState RAFT_STATE;

    private static final String HEARTBEAT_TIMEOUT_NAME = "HeartBeat_Timeout";
    private static final String ELECTION_TIMEOUT_NAME = "Election_Timeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;

    private final ConcurrentLinkedDeque<RaftLogEntry> raftLogEntryDeque = new ConcurrentLinkedDeque<>();
    private final RaftConfiguration raftConfiguration;
    private final HashSet<SelectionKey> votedNodes = new HashSet<>();

    private final StateMachine stateMachine;

    private final Timeout heartbeat;
    private final Timeout election;

    private final ServerNode currentNode;

    /**
     * 已提交的日志索引
     */
    private final AtomicInteger commitIndex = new AtomicInteger(0);

    /**
     * 应用到状态机的日志索引
     */
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    private final ConcurrentHashMap<SelectionKey, Integer> nextIndex
            = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<SelectionKey, Integer> matchedIndex
            = new ConcurrentHashMap<>();

    /**
     * Raft 日志
     */
    private final LogFileGroup raftLog;


    private volatile ServerNode leaderNode;
    private volatile RaftRole raftRole = RaftRole.FOLLOWER;

    private RaftClient raftClient;
    private RaftServer raftServer;

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
            RAFT_STATE = new RaftState(stateMachine.get(), raftConfiguration.get());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static RaftState getInstance() {
        return RAFT_STATE;
    }

    private RaftState(StateMachine stateMachine, RaftConfiguration raftConfiguration)
            throws IOException {
        this.stateMachine = stateMachine;
        this.raftConfiguration = raftConfiguration;

        currentNode = raftConfiguration.currentNode();

        // 日志
        raftLog = new LogFileGroup(raftConfiguration.logDir());

        // 超时计时器
        heartbeat = new Timeout(new HeartbeatTask(), HEARTBEAT_INTERVAL_MILLIS);
        // 设置随机选举超时时间
        final int ELECTION_INTERVAL_MILLIS = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;
        election = new Timeout(new ElectionTask(), ELECTION_INTERVAL_MILLIS);
    }

    public void startTimeout() {
        String electionMode = raftConfiguration.electionMode();

        // 如果 leaderMode 为 follower，不需要启动超时计时器
        if(RaftConfiguration.FOLLOWER.equals(electionMode)) {
            logger.info("Election Mode is [{}], Do not start Timeout.", electionMode);
        } else if (RaftConfiguration.LEADER.equals(electionMode)
                || RaftConfiguration.CANDIDATE.equals(electionMode)) {
            logger.info("Election Mode is [{}].", electionMode);

            // 启动心跳超时计时器
            new Thread(heartbeat, HEARTBEAT_TIMEOUT_NAME).start();
            // 启动选举超时计时器
            new Thread(election, ELECTION_TIMEOUT_NAME).start();
        }
    }

    public ServerNode currentNode() {
        return currentNode;
    }

    public void appendEntry(RaftLogEntry entry) {
        // 写入日志文件
        raftLog.append(entry);
        // 加入双向队列
        raftLogEntryDeque.addFirst(entry);
    }

    public void sendRequestVote() {

    }

    public void sendAppendEntries() {
        if(nextIndex.isEmpty()) {
            RaftLogEntry entry = raftLogEntryDeque.pollLast();
            stateMachine.apply(new RaftLogEntry[]{entry});
            return;
        }

        for(SelectionKey key : nextIndex.keySet()) {

        }
    }

    public ServerNode leaderNode() {
        return leaderNode;
    }

    public void leaderNode(ServerNode leaderNode) {
        this.leaderNode = leaderNode;
    }

    public void raftRole(RaftRole role) {
        RaftRole oldRaftRole = raftRole;
        raftRole = role;

        logger.info("Transform raftRole from [{}] to [{}]",
                oldRaftRole, raftRole);
    }

    public RaftRole raftRole() {
        return raftRole;
    }
    public synchronized void transformToCandidate() {
        raftRole(RaftRole.CANDIDATE);
    }


    public int commitIndex() {
        return commitIndex.get();
    }
    public void setCommitIndex(int index) {
        commitIndex.set(index);
    }
    public int lastApplied() {
        return lastApplied.get();
    }

    /**
     * 将日志条目应用到状态机
     * @param entries 日志条目
     */
    public void apply(RaftLogEntry[] entries) {
        stateMachine.apply(entries);
        lastApplied.set(commitIndex.get());
    }

    public void resetElectionTimeout() {
        election.reset();
    }

    public void resetHeartbeatTimeout() {
        heartbeat.reset();
    }

    public void raftClient(RaftClient client) {
        if(raftClient == null) {
            raftClient = client;
        }
    }

    public void raftServer(RaftServer server) {
        if(raftServer == null) {
            raftServer = server;
            stateMachine.raftServer(server);
        }
    }
}
