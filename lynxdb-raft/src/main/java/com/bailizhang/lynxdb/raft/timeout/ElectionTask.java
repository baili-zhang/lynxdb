package com.bailizhang.lynxdb.raft.timeout;

import com.bailizhang.lynxdb.core.timeout.TimeoutTask;
import com.bailizhang.lynxdb.raft.state.RaftState;

import static com.bailizhang.lynxdb.raft.state.RaftRole.CANDIDATE;
import static com.bailizhang.lynxdb.raft.state.RaftRole.FOLLOWER;

public class ElectionTask implements TimeoutTask {
    private final RaftState raftState;

    public ElectionTask() {
        raftState = RaftState.getInstance();
    }

    @Override
    public void execute() {
        if(raftState.raftRole() == FOLLOWER || raftState.raftRole() == CANDIDATE) {
            raftState.transformToCandidate();
            raftState.sendRequestVote();
        }
    }
}
