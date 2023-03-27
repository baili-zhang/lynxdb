package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.common.RaftRole;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.PreVote;
import com.bailizhang.lynxdb.raft.request.PreVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.utils.SpiUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class RaftRpcHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftRpcHandler.class);

    private static final String HEARTBEAT_TIMEOUT = "heartbeatTimeout";
    private static final String ELECTION_TIMEOUT = "electionTimeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;


    private final int electionIntervalMillis;
    private final RaftClient client;

    private final LogGroup raftLog;

    private final StateMachine stateMachine;
    private final RaftConfiguration raftConfig;

    private TimeoutTask electionTask;
    private TimeoutTask heartbeatTask;

    public RaftRpcHandler() {
        electionIntervalMillis = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;

        logger.info("Election interval time: {} ms", electionIntervalMillis);

        client = RaftClient.client();

        stateMachine = SpiUtils.serviceLoad(StateMachine.class);
        raftConfig = SpiUtils.serviceLoad(RaftConfiguration.class);

        LogGroupOptions options = new LogGroupOptions(INT_LENGTH);
        raftLog = new LogGroup(raftConfig.logsDir(), options);

        if(raftLog.maxGlobalIdx() == 0) {
            stateMachine.currentTerm(1);
        }

        RaftState raftState = RaftStateHolder.raftState();

        int currentTerm = stateMachine.currentTerm();
        raftState.currentTerm().set(currentTerm);
    }

    public RequestVoteResult handlePreVote(
            int term,
            ServerNode candidate,
            int lastLogIndex,
            int lastLogTerm
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();

        if(term <= currentTerm) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        int lastIndex = raftLog.maxGlobalIdx();
        int lastTerm = ByteArrayUtils.toInt(raftLog.lastExtraData());

        if(lastLogIndex > lastIndex || (lastLogIndex == lastIndex && lastLogTerm > lastTerm)) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
        }

        return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
    }

    public RequestVoteResult handleRequestVote(
            int term,
            ServerNode candidate,
            int lastLogIndex,
            int lastLogTerm
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();
        ServerNode votedFor = raftState.voteFor().get();

        if(term < currentTerm || (term == currentTerm && votedFor != null)) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        int lastIndex = raftLog.maxGlobalIdx();
        int lastTerm = ByteArrayUtils.toInt(raftLog.lastExtraData());

        if(lastLogIndex > lastIndex || (lastLogIndex == lastIndex && lastLogTerm > lastTerm)) {
            if(raftState.voteFor().compareAndSet(votedFor, candidate)) {
                return new RequestVoteResult(currentTerm, RequestVoteResult.IS_VOTE_GRANTED);
            }
        }

        return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
    }

    public AppendEntriesResult handleAppendEntries(
            int term,
            ServerNode leader,
            int prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            int leaderCommit
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();
        ServerNode leaderNode = raftState.leader().get();

        if(term < currentTerm || (leaderNode != null && !leaderNode.equals(leader))) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        int preIndex = raftLog.maxGlobalIdx();
        int preTerm = ByteArrayUtils.toInt(raftLog.lastExtraData());

        if(preIndex != prevLogIndex || preTerm != prevLogTerm) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        List<byte[]> commends = entries.stream().map(LogEntry::data).toList();

//        stateMachine.apply0(commends);
//        commitIndex.set(leaderCommit);

        return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
    }

    public InstallSnapshotResult handleInstallSnapshot(
            int term,
            ServerNode leader,
            int lastIncludedIndex,
            int lastIncludedTerm,
            int offset,
            byte[] data,
            byte done
    ) {
        return null;
    }

    public void registerTimeoutTasks() {
        long current = System.currentTimeMillis();
        long electionTime = current + electionIntervalMillis;
        long heartbeatTime = current + HEARTBEAT_INTERVAL_MILLIS;

        SocketTimeWheel timeWheel = SocketTimeWheel.timeWheel();

        electionTask = timeWheel.register(electionTime, ELECTION_TIMEOUT, this::election);
        heartbeatTask = timeWheel.register(heartbeatTime, HEARTBEAT_TIMEOUT, this::heartbeat);

        logger.info("Register election and heartbeat timeout task.");
    }

    public void connectClusterMembers() {
        RaftState raftState = RaftStateHolder.raftState();
        List<ServerNode> members = stateMachine.clusterMembers();

        members.forEach(member -> {
            // 不需要连接自己
            if(member.equals(raftConfig.currentNode())) {
                return;
            }

            try {
                LynxDbFuture<SelectionKey> future = client.connect(member);
                raftState.matchedIndex().put(future.get(), 0);
            } catch (IOException e) {
                logger.error("Connect member {} failed.", member, e);
            }
        });
    }

    private void election() {
        List<ServerNode> nodes = stateMachine.clusterMembers();

        logger.info("Run election task, time: {}, cluster nodes: {}",
                System.currentTimeMillis(), nodes);

        ServerNode current = raftConfig.currentNode();
        RaftState raftState = RaftStateHolder.raftState();
        AtomicReference<RaftRole> role = raftState.role();

        String runningMode = raftConfig.electionMode();

        // 如果 runningMode 是 follow，则不需要启动下一轮的超时计时器
        if(RunningMode.FOLLOWER.equals(runningMode)) {
            return;
        }

        if(nodes.isEmpty() && RunningMode.LEADER.equals(runningMode)) {
            // 如果当前没有节点，并且 runningMode 为 leader
            // 则直接将当前节点的角色升级成 leader
            if(role.compareAndSet(null, RaftRole.LEADER)) {
                stateMachine.addClusterMember(current);
                logger.info("Upgrade raft role to leader.");
                return;
            }
        }

        // 转换成 candidate, 并发起预投票
        role.set(RaftRole.CANDIDATE);

        int term = raftState.currentTerm().get();

        byte[] extraData = raftLog.lastExtraData();
        int lastLogTerm = extraData == null ? 0 : ByteArrayUtils.toInt(extraData);

        PreVoteArgs args = new PreVoteArgs(
                term + 1,
                raftLog.maxGlobalIdx(),
                lastLogTerm
        );

        PreVote preVote = new PreVote(args);
        client.broadcast(preVote);

        SocketTimeWheel timeWheel = SocketTimeWheel.timeWheel();

        long nextElectionTime = electionTask.time() + electionIntervalMillis;
        electionTask = timeWheel.register(nextElectionTime, ELECTION_TIMEOUT, this::election);
    }

    private void heartbeat() {
        logger.info("Run heartbeat task, time: {}", System.currentTimeMillis());

        RaftState raftState = RaftStateHolder.raftState();
        int term = raftState.currentTerm().get();
        int leaderCommit = raftState.commitIndex().get();

        var matchedIndex = raftState.matchedIndex();

        matchedIndex.forEach(((selectionKey, commit) -> {
            AppendEntriesArgs args = new AppendEntriesArgs(
                    term,
                    raftConfig.currentNode(),
                    raftLog.maxGlobalIdx(),
                    ByteArrayUtils.toInt(raftLog.lastExtraData()),
                    new ArrayList<>(),
                    leaderCommit
            );

            AppendEntries appendEntries = new AppendEntries(selectionKey, args);
            client.send(appendEntries);
        }));
    }
}
