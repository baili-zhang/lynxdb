package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.nio.channels.SelectionKey;
import java.util.List;

public class RaftRpcHandler {
    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;


    private final int electionIntervalMillis;

    private TimeoutTask electionTask;
    private TimeoutTask heartbeatTask;

    public RaftRpcHandler() {
        electionIntervalMillis = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;
    }

    public RequestVoteResult handlePreVote(
            int term,
            ServerNode candidate, //TODO
            int lastLogIndex,
            int lastLogTerm
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();
        ServerNode votedFor = raftState.voteFor().get();
        LogGroup log = raftState.log();

        if(term < currentTerm || (term == currentTerm && votedFor != null)) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        int lastIndex = log.maxGlobalIndex();
        int lastTerm = ByteArrayUtils.toInt(log.lastLogExtraData());

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
        LogGroup log = raftState.log();

        if(term < currentTerm || (term == currentTerm && votedFor != null)) {
            return new RequestVoteResult(currentTerm, RequestVoteResult.NOT_VOTE_GRANTED);
        }

        int lastIndex = log.maxGlobalIndex();
        int lastTerm = ByteArrayUtils.toInt(log.lastLogExtraData());

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
        LogGroup log = raftState.log();

        if(term < currentTerm || (leaderNode != null && !leaderNode.equals(leader))) {
            return new AppendEntriesResult(currentTerm, AppendEntriesResult.IS_FAILED);
        }

        int preIndex = log.maxGlobalIndex();
        int preTerm = ByteArrayUtils.toInt(log.lastLogExtraData());

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
    }

    private void election() {

    }

    private void heartbeat() {

    }
}
