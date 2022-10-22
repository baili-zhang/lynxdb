package com.bailizhang.lynxdb.raft.timeout;

import com.bailizhang.lynxdb.core.timeout.TimeoutTask;
import com.bailizhang.lynxdb.raft.state.RaftState;

import java.util.concurrent.atomic.AtomicInteger;

public class ElectionTask implements TimeoutTask {
    private final RaftState raftState;
    private final AtomicInteger electionTimeoutTimes;

    public ElectionTask(AtomicInteger times) {
        raftState = RaftState.getInstance();
        electionTimeoutTimes = times;
    }

    @Override
    public void execute() {
        if(raftState.isFollower() || raftState.isCandidate()) {
            raftState.transformToCandidate();
            raftState.sendRequestVote();
            electionTimeoutTimes.getAndIncrement();
        }
    }
}
