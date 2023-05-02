package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.PreVote;
import com.bailizhang.lynxdb.raft.request.PreVoteArgs;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;
import com.bailizhang.lynxdb.raft.spi.StateMachine;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RaftTimeWheel {
    private static final Logger logger = LoggerFactory.getLogger(RaftTimeWheel.class);

    private static final String HEARTBEAT_TIMEOUT = "heartbeatTimeout";
    private static final String ELECTION_TIMEOUT = "electionTimeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;

    private static final RaftTimeWheel RAFT_TIME_WHEEL = new RaftTimeWheel();

    private final RaftConfiguration raftConfig;
    private final RaftClient client;
    private final StateMachine stateMachine;
    private final RaftState raftState;
    private final RaftLog raftLog;

    private final LynxDbTimeWheel timeWheel;

    private final int electionIntervalMillis;

    private volatile TimeoutTask electionTask;
    private volatile TimeoutTask heartbeatTask;

    public static RaftTimeWheel timeWheel() {
        return RAFT_TIME_WHEEL;
    }

    private RaftTimeWheel() {
        raftConfig = RaftSpiService.raftConfig();
        client = RaftClient.client();
        stateMachine = RaftSpiService.stateMachine();
        raftState = RaftStateHolder.raftState();
        raftLog = RaftLog.raftLog();

        timeWheel = new LynxDbTimeWheel();

        electionIntervalMillis = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;

        logger.info("Election interval time: {} ms", electionIntervalMillis);
    }

    public void registerElectionTimeoutTask() {
        long current = System.currentTimeMillis();
        long electionTime = current + electionIntervalMillis;

        electionTask = new TimeoutTask(electionTime, ELECTION_TIMEOUT, this::election);
        timeWheel.register(electionTask);

        logger.info("Register election timeout task.");
    }

    public void connectNotConnectedMembers() {
        List<ServerNode> members = stateMachine.clusterMembers();

        members.forEach(member -> {
            // 不需要连接自己
            if(member.equals(raftConfig.currentNode())) {
                return;
            }

            if(client.isConnected(member)) {
                return;
            }

            logger.trace("Try to connect member {}", member);

            try {
                LynxDbFuture<SelectionKey> future = client.connect(member);
                SelectionKey key = future.get();

                if(key.isValid()) {
                    raftState.matchedIndex().put(key, 0);
                    return;
                }
            } catch (IOException e) {
                logger.error("Connect member {} catch exception.", member, e);
            }

            logger.trace("Connect member {} failed.", member);
        });
    }

    private void election() {
        // 尝试连接未连接的节点
        connectNotConnectedMembers();

        List<ServerNode> nodes = stateMachine.clusterMembers();

        logger.trace("Run election task, time: {}, cluster nodes: {}",
                System.currentTimeMillis(), nodes);

        AtomicReference<RaftRole> role = raftState.role();

        // 转换成 candidate, 并发起预投票
        role.set(RaftRole.CANDIDATE);

        int term = raftState.currentTerm().get();

        PreVoteArgs args = new PreVoteArgs(
                term + 1,
                raftLog.maxIndex(),
                raftLog.maxTerm()
        );

        PreVote preVote = new PreVote(args);
        client.broadcast(preVote);

        long nextElectionTime = electionTask.time() + electionIntervalMillis;
        electionTask = new TimeoutTask(nextElectionTime, ELECTION_TIMEOUT, this::election);
        timeWheel.register(electionTask);
    }

    /**
     * TODO: 线程安全需要测试和分析
     */
    public synchronized void heartbeat() {
        timeWheel.unregister(heartbeatTask);

        // 连接集群中的还未连接上的其他节点
        connectNotConnectedMembers();

        logger.info("Run heartbeat task, time: {}", System.currentTimeMillis());

        int term = raftState.currentTerm().get();
        AtomicInteger commitIndex = raftState.commitIndex();
        int lastCommitIndex = commitIndex.get();

        var matchedIndex = raftState.matchedIndex();
        int lastLogTerm = raftLog.maxTerm();

        List<SelectionKey> invalid = new ArrayList<>();

        // 如果有 follower 节点，则需要发送 AppendEntries 请求
        matchedIndex.forEach(((selectionKey, commit) -> {
            if(!selectionKey.isValid()) {
                invalid.add(selectionKey);
                return;
            }

            AppendEntriesArgs args = new AppendEntriesArgs(
                    term,
                    raftConfig.currentNode(),
                    raftLog.maxIndex(),
                    lastLogTerm,
                    new ArrayList<>(),
                    lastCommitIndex
            );

            AppendEntries appendEntries = new AppendEntries(selectionKey, args);
            client.send(appendEntries);
        }));

        invalid.forEach(matchedIndex::remove);

        long nextHeartbeatTime = heartbeatTask.time() + HEARTBEAT_INTERVAL_MILLIS;
        heartbeatTask = new TimeoutTask(nextHeartbeatTime, HEARTBEAT_TIMEOUT, this::heartbeat);
        timeWheel.register(heartbeatTask);
    }

    public void start() {
        Executor.startRunnable(timeWheel);
    }
}
