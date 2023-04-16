package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.PreVote;
import com.bailizhang.lynxdb.raft.request.PreVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;
import com.bailizhang.lynxdb.raft.spi.StateMachine;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

    private TimeoutTask electionTask;
    private volatile TimeoutTask heartbeatTask;

    public RaftRpcHandler() {
        electionIntervalMillis = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;

        logger.info("Election interval time: {} ms", electionIntervalMillis);

        client = RaftClient.client();

        RaftConfiguration raftConfig = RaftSpiService.raftConfig();
        StateMachine stateMachine = RaftSpiService.stateMachine();

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
            int lastLogIndex,
            int lastLogTerm
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();

        if(term <= currentTerm) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        int lastIndex = raftLog.maxGlobalIdx();

        byte[] extraData = raftLog.lastExtraData();
        int lastTerm = extraData == null ? 0 : ByteArrayUtils.toInt(extraData);

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

    public int persistenceClientRequest(byte[] data) {
        RaftState raftState = RaftStateHolder.raftState();
        int term = raftState.currentTerm().get();
        return raftLog.append(BufferUtils.toBytes(term), data);
    }

    public void registerElectionTimeoutTask() {
        long current = System.currentTimeMillis();
        long electionTime = current + electionIntervalMillis;

        SocketTimeWheel timeWheel = SocketTimeWheel.timeWheel();

        electionTask = timeWheel.register(electionTime, ELECTION_TIMEOUT, this::election);

        logger.info("Register election timeout task.");
    }

    public void connectNotConnectedMembers() {
        RaftState raftState = RaftStateHolder.raftState();
        RaftConfiguration raftConfig = RaftSpiService.raftConfig();
        StateMachine stateMachine = RaftSpiService.stateMachine();

        List<ServerNode> members = stateMachine.clusterMembers();

        members.forEach(member -> {
            // 不需要连接自己
            if(member.equals(raftConfig.currentNode())) {
                return;
            }

            if(client.isConnected(member)) {
                return;
            }

            logger.info("Try to connect member {}", member);

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

            logger.error("Connect member {} failed.", member);
        });
    }

    private void election() {
        // 尝试连接未连接的节点
        connectNotConnectedMembers();

        RaftState raftState = RaftStateHolder.raftState();
        StateMachine stateMachine = RaftSpiService.stateMachine();

        List<ServerNode> nodes = stateMachine.clusterMembers();

        logger.info("Run election task, time: {}, cluster nodes: {}",
                System.currentTimeMillis(), nodes);

        AtomicReference<RaftRole> role = raftState.role();

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

    /**
     * TODO: 线程安全需要测试和分析
     */
    public synchronized void heartbeat() {
        SocketTimeWheel timeWheel = SocketTimeWheel.timeWheel();
        timeWheel.unregister(heartbeatTask);

        RaftState raftState = RaftStateHolder.raftState();
        RaftConfiguration raftConfig = RaftSpiService.raftConfig();

        // 连接集群中的还未连接上的其他节点
        connectNotConnectedMembers();

        logger.info("Run heartbeat task, time: {}", System.currentTimeMillis());

        int term = raftState.currentTerm().get();
        AtomicInteger commitIndex = raftState.commitIndex();
        int lastCommitIndex = commitIndex.get();

        var matchedIndex = raftState.matchedIndex();

        byte[] extraData = raftLog.lastExtraData();
        int lastLogTerm = extraData == null ? 0 : ByteArrayUtils.toInt(extraData);

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
                    raftLog.maxGlobalIdx(),
                    lastLogTerm,
                    new ArrayList<>(),
                    lastCommitIndex
            );

            AppendEntries appendEntries = new AppendEntries(selectionKey, args);
            client.send(appendEntries);
        }));

        invalid.forEach(matchedIndex::remove);

        long nextHeartbeatTime = heartbeatTask.time() + HEARTBEAT_INTERVAL_MILLIS;
        heartbeatTask = timeWheel.register(nextHeartbeatTime, HEARTBEAT_TIMEOUT, this::heartbeat);
    }
}
