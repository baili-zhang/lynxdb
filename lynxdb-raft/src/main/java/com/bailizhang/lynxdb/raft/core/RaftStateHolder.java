package com.bailizhang.lynxdb.raft.core;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RaftStateHolder {
    public static final RaftState raftState = new RaftState(
            new AtomicReference<>(),
            new AtomicInteger(),
            new AtomicInteger(),
            new HashSet<>(),
            new ConcurrentHashMap<>(),
            new ConcurrentHashMap<>(),
            new AtomicReference<>()
    );

    public static RaftState raftState() {
        return raftState;
    }
}
