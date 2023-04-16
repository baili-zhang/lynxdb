package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.request.RequestVote;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;
import com.bailizhang.lynxdb.raft.spi.StateMachine;

import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.TRUE;

public class RaftRpcResultHandler {
    private final RaftClient client;
    private final RaftState raftState;
    private final StateMachine stateMachine;
    private final RaftConfiguration raftConfig;
    private final RaftLog raftLog;

    public RaftRpcResultHandler() {
        client = RaftClient.client();
        raftState = RaftStateHolder.raftState();
        stateMachine = RaftSpiService.stateMachine();
        raftConfig = RaftSpiService.raftConfig();
        raftLog = RaftLog.raftLog();
    }

    public void handlePreVoteResult(
            SelectionKey selectionKey,
            int term,
            byte voteGranted
    ) {
        if(voteGranted != TRUE) {
            return;
        }

        int currentTerm = raftState.currentTerm().incrementAndGet();
        stateMachine.currentTerm(currentTerm);

        RequestVoteArgs args = new RequestVoteArgs(
                currentTerm,
                raftConfig.currentNode(),
                raftLog.maxIndex(),
                raftLog.maxTerm()
        );

        RequestVote requestVote = new RequestVote(args);
        client.broadcast(requestVote);
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
}
