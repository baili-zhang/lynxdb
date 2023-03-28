package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public record RaftState(
        AtomicReference<RaftRole> role,
        AtomicInteger currentTerm,
        AtomicInteger commitIndex,
        HashSet<SelectionKey> votedNodes,
        ConcurrentHashMap<SelectionKey, Integer> nextIndex,
        ConcurrentHashMap<SelectionKey, Integer> matchedIndex,
        AtomicReference<ServerNode> voteFor,
        AtomicReference<ServerNode> leader
) {
}
