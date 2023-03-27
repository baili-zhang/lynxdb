package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.raft.utils.SpiUtils;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.raft.request.AppendEntries;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.RequestVote;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.INT_LENGTH;

public class RaftRpcHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftRpcHandler.class);

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

    public RaftRpcHandler(RaftClient raftClient) {
        electionIntervalMillis = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;

        logger.info("Election interval time: {} ms", electionIntervalMillis);

        client = raftClient;

        stateMachine = SpiUtils.serviceLoad(StateMachine.class);
        raftConfig = SpiUtils.serviceLoad(RaftConfiguration.class);

        LogGroupOptions options = new LogGroupOptions(INT_LENGTH);
        raftLog = new LogGroup(raftConfig.logDir(), options);

        List<ServerNode> members = stateMachine.clusterMembers();
        RaftState raftState = RaftStateHolder.raftState();
        members.forEach(member -> {
            try {
                LynxDbFuture<SelectionKey> future = raftClient.connect(member);
                raftState.matchedIndex().put(future.get(), 0);
            } catch (IOException e) {
                logger.error("Connect member {} failed.", member, e);
            }
        });
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

        int lastIndex = raftLog.maxGlobalIndex();
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

        int lastIndex = raftLog.maxGlobalIndex();
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

        int preIndex = raftLog.maxGlobalIndex();
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

    public void handleRequestVoteResult(
            SelectionKey selectionKey,
            int term,
            byte voteGranted
    ) {

    }

    public void handleAppendEntriesResult(
            SelectionKey selectionKey,
            int term,
            byte voteGranted
    ) {

    }

    public void handleInstallSnapshotResult(SelectionKey selectionKey, int term) {
    }

    public void registerTimeoutTasks() {
        long current = System.currentTimeMillis();
        long electionTime = current + electionIntervalMillis;
        long heartbeatTime = current + HEARTBEAT_INTERVAL_MILLIS;

        SocketTimeWheel timeWheel = SocketTimeWheel.timeWheel();
        timeWheel.register(electionTime, this::election);
        timeWheel.register(heartbeatTime, this::heartbeat);

        logger.info("Register election and heartbeat timeout task.");
    }

    private void election() {
        logger.info("Run election task, time: {}", System.currentTimeMillis());

        RaftState raftState = RaftStateHolder.raftState();
        int term = raftState.currentTerm().get();

        byte[] extraData = raftLog.lastExtraData();
        int lastLogTerm = extraData == null ? 0 : ByteArrayUtils.toInt(extraData);

        RequestVoteArgs args = new RequestVoteArgs(
                term + 1,
                raftConfig.currentNode(),
                raftLog.maxGlobalIndex(),
                lastLogTerm
        );

        RequestVote requestVote = new RequestVote(args);
        client.broadcast(requestVote);
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
                    raftLog.maxGlobalIndex(),
                    ByteArrayUtils.toInt(raftLog.lastExtraData()),
                    new ArrayList<>(),
                    leaderCommit
            );

            AppendEntries appendEntries = new AppendEntries(selectionKey, args);
            client.send(appendEntries);
        }));
    }
}
