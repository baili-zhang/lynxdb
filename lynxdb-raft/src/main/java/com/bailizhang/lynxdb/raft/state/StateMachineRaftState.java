package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.common.RaftLogEntry;

import java.util.concurrent.atomic.AtomicInteger;

public class StateMachineRaftState extends RpcRaftState {
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    public void apply(RaftLogEntry[] entries) {
        stateMachine.apply(entries);
        lastApplied.set(commitIndex.get());
    }

    public int lastApplied() {
        return lastApplied.get();
    }
}
