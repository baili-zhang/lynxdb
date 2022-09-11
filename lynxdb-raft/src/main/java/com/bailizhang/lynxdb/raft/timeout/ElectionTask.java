package com.bailizhang.lynxdb.raft.timeout;

import com.bailizhang.lynxdb.core.timeout.TimeoutTask;
import com.bailizhang.lynxdb.raft.state.RaftState;

public class ElectionTask implements TimeoutTask {
    private final RaftState raftState;

    public ElectionTask() {
        raftState = RaftState.getInstance();
    }

    @Override
    public void execute() {
        if(raftState.isFollower() || raftState.isCandidate()) {
            raftState.transformToCandidate();
            raftState.sendRequestVote();
        }
    }
}
