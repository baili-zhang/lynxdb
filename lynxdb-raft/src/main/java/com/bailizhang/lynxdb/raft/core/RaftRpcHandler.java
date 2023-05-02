package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.core.common.CheckThreadSafety;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.PreVoteResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;
import com.bailizhang.lynxdb.raft.spi.StateMachine;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.FALSE;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.TRUE;

public class RaftRpcHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftRpcHandler.class);

    private final RaftState raftState;
    private final StateMachine stateMachine;
    private final RaftLog raftLog;

    public RaftRpcHandler() {
        stateMachine = RaftSpiService.stateMachine();

        raftLog = RaftLog.raftLog();

        if(raftLog.maxIndex() == 0) {
            stateMachine.currentTerm(0);
        }

        raftState = RaftStateHolder.raftState();

        int currentTerm = stateMachine.currentTerm();
        raftState.currentTerm().set(currentTerm);
    }

    @CheckThreadSafety
    public PreVoteResult handlePreVote(
            int term,
            int lastLogIndex,
            int lastLogTerm
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        int currentTerm = raftState.currentTerm().get();

        if(term <= currentTerm) {
            return new PreVoteResult(currentTerm, FALSE);
        }

        int lastIndex = raftLog.maxIndex();
        int maxLogTerm = raftLog.maxTerm();

        if(lastLogIndex >= lastIndex && lastLogTerm >= maxLogTerm) {
            return new PreVoteResult(currentTerm, TRUE);
        }

        return new PreVoteResult(currentTerm, FALSE);
    }

    @CheckThreadSafety
    public RequestVoteResult handleRequestVote(
            int term,
            ServerNode candidate,
            int lastLogIndex,
            int lastLogTerm
    ) {
        int currentTerm = raftState.currentTerm().get();
        if(term < currentTerm) {
            return new RequestVoteResult(currentTerm, FALSE);
        }

        int lastIndex = raftLog.maxIndex();
        int lastTerm = raftLog.maxTerm();

        if(lastLogIndex >= lastIndex && lastLogTerm >= lastTerm
                && stateMachine.voteForIfNull(term, candidate)) {
            return new RequestVoteResult(currentTerm, TRUE);
        }

        return new RequestVoteResult(currentTerm, FALSE);
    }

    @CheckThreadSafety
    public AppendEntriesResult handleAppendEntries(
            int term,
            ServerNode leader,
            int prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            int leaderCommit
    ) {
        int currentTerm = raftState.currentTerm().get();
        ServerNode leaderNode = raftState.leader().get();

        if(term < currentTerm || (leaderNode != null && !leaderNode.equals(leader))) {
            return new AppendEntriesResult(currentTerm, FALSE);
        }

        int preIndex = raftLog.maxIndex();
        int preTerm = raftLog.maxTerm();

        if(preIndex != prevLogIndex || preTerm != prevLogTerm) {
            return new AppendEntriesResult(currentTerm, FALSE);
        }

        List<byte[]> commends = entries.stream().map(LogEntry::data).toList();

        return new AppendEntriesResult(currentTerm, FALSE);
    }

    @CheckThreadSafety
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

    @CheckThreadSafety
    public int persistenceClientRequest(byte[] data) {
        int term = raftState.currentTerm().get();
        return raftLog.append(term, data);
    }
}
