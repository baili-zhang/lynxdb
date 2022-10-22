package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.raft.common.AppliableLogEntry;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StateMachineRaftState extends RpcRaftState {
    private final AtomicInteger lastApplied = new AtomicInteger(0);

    public void apply(List<AppliableLogEntry> requests) {
        stateMachine.apply(requests);
        lastApplied.set(commitIndex.get());
    }

    public int lastApplied() {
        return lastApplied.get();
    }
}
