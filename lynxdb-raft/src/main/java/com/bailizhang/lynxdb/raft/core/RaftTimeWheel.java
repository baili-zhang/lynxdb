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
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RaftTimeWheel {
    private static final Logger logger = LoggerFactory.getLogger(RaftTimeWheel.class);

    private static final String HEARTBEAT_TIMEOUT = "heartbeatTimeout";
    private static final String ELECTION_TIMEOUT = "electionTimeout";
    private static final String CONNECT_MEMBERS_TIMEOUT = "connectMemberTimeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;
    private static final int CONNECT_MEMBERS_INTERVAL_MILLIS = 1000;

    private static final RaftTimeWheel RAFT_TIME_WHEEL = new RaftTimeWheel();

    private final RaftConfiguration raftConfig;
    private final RaftClient client;
    private final StateMachine stateMachine;
    private final RaftState raftState;
    private final RaftLog raftLog;

    private final LynxDbTimeWheel timeWheel;

    private final int electionIntervalMillis;

    private final AtomicReference<TimeoutTask> electionTask;
    private final AtomicReference<TimeoutTask> heartbeatTask;
    private final AtomicReference<TimeoutTask> connectMemberTask;

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

        electionTask = new AtomicReference<>();
        heartbeatTask = new AtomicReference<>();
        connectMemberTask = new AtomicReference<>();

        logger.info("Election interval time: {} ms", electionIntervalMillis);
    }

    public void connectMembers() {
        resetConnectMembers();

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
            } catch (IOException | CancellationException e) {
                logger.trace("Connect member {} catch exception.", member, e);
            }

            logger.trace("Connect member {} failed.", member);
        });
    }

    private void election() {
        resetElection();

        List<ServerNode> nodes = stateMachine.clusterMembers();

        logger.trace("Run election task, time: {}, cluster nodes: {}",
                System.currentTimeMillis(), nodes);

        AtomicReference<RaftRole> role = raftState.role();

        // 转换成 candidate, 并发起预投票
        role.set(RaftRole.CANDIDATE);

        int term = stateMachine.currentTerm();

        PreVoteArgs args = new PreVoteArgs(
                term,
                raftLog.maxIndex(),
                raftLog.maxTerm()
        );

        PreVote preVote = new PreVote(args);
        client.broadcast(preVote);

        logger.trace("Send PRE VOTE rpc to cluster members.");
    }

    public void heartbeat() {
        resetHeartbeat();

        logger.trace("Run heartbeat task, time: {}", System.currentTimeMillis());

        int term = stateMachine.currentTerm();
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
    }

    public void resetElection() {
        long runTaskTime = System.currentTimeMillis() + electionIntervalMillis;

        if(electionTask.get() == null) {
            TimeoutTask task = new TimeoutTask(
                    runTaskTime,
                    ELECTION_TIMEOUT,
                    this::election
            );

            electionTask.set(task);
            timeWheel.register(task);
            logger.info("Register election timeout task.");
            return;
        }

        electionTask.set(timeWheel.reset(electionTask.get(), runTaskTime));
        logger.trace("Reset election timeout task.");
    }

    public void resetHeartbeat() {
        long runTaskTime = System.currentTimeMillis() + HEARTBEAT_INTERVAL_MILLIS;

        if(heartbeatTask.get() == null) {
            TimeoutTask task = new TimeoutTask(
                    runTaskTime,
                    HEARTBEAT_TIMEOUT,
                    this::heartbeat
            );

            heartbeatTask.set(task);
            timeWheel.register(task);
            logger.info("Register heartbeat timeout task.");
            return;
        }

        heartbeatTask.set(timeWheel.reset(heartbeatTask.get(), runTaskTime));
        logger.trace("Reset heartbeat timeout task, resetTime: {}", runTaskTime);
    }

    public void resetConnectMembers() {
        long runTaskTime = System.currentTimeMillis() + CONNECT_MEMBERS_INTERVAL_MILLIS;

        if(connectMemberTask.get() == null) {
            TimeoutTask task = new TimeoutTask(
                    runTaskTime,
                    CONNECT_MEMBERS_TIMEOUT,
                    this::connectMembers
            );

            connectMemberTask.set(task);
            timeWheel.register(task);
            logger.info("Register connect member timeout task.");
            return;
        }

        connectMemberTask.set(timeWheel.reset(connectMemberTask.get(), runTaskTime));
        logger.trace("Reset connect member timeout task.");
    }

    public void start() {
        Executor.startRunnable(timeWheel);
    }
}
