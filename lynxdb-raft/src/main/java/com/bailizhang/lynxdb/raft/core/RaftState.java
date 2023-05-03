package com.bailizhang.lynxdb.raft.core;

import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.channels.SelectionKey;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public record RaftState(
        AtomicReference<RaftRole> role,
        AtomicInteger commitIndex,
        HashSet<SelectionKey> votedNodes,
        ConcurrentHashMap<SelectionKey, Integer> nextIndex,
        ConcurrentHashMap<SelectionKey, Integer> matchedIndex,
        AtomicReference<ServerNode> leader
) {
    private static final Logger logger = LoggerFactory.getLogger(RaftState.class);

    public void changeRoleToLeader() {
        role.compareAndSet(RaftRole.CANDIDATE, RaftRole.LEADER);
        RaftTimeWheel.timeWheel().resetHeartbeat();

        logger.info("Upgrade role from candidate to leader.");
    }

    public void changeRoleToCandidate() {

    }
}
