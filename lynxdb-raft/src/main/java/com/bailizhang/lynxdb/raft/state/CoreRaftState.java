package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.common.RaftRole;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CoreRaftState extends TimeoutRaftState {
    private static final Logger logger = LogManager.getLogger("TimeoutRaftState");

    private static final String CURRENT_TERM = "current_term";

    protected final ServerNode currentNode;

    protected volatile ServerNode leaderNode;
    protected volatile RaftRole raftRole = RaftRole.FOLLOWER;
    protected volatile int currentTerm;

    protected CoreRaftState() {
        currentNode = raftConfiguration.currentNode();
    }

    public void checkTerm(int term) {
        if(term > currentTerm) {
            currentTerm(term);
            transformToFollower();
        }
    }

    public void currentTerm(int term) {
        stateMachine.metaSet(CURRENT_TERM, BufferUtils.toBytes(term));
        currentTerm = term;
    }

    public ServerNode currentNode() {
        return currentNode;
    }

    public synchronized void transformToCandidate() {
        raftRole(RaftRole.CANDIDATE);
    }

    public synchronized void transformToLeader() {
        raftRole(RaftRole.LEADER);
    }

    public synchronized void transformToFollower() {
        raftRole(RaftRole.FOLLOWER);
    }

    public boolean isLeader() {
        return raftRole == RaftRole.LEADER;
    }

    public boolean isCandidate() {
        return raftRole == RaftRole.CANDIDATE;
    }

    public boolean isFollower() {
        return raftRole == RaftRole.FOLLOWER;
    }

    public boolean hasLeader() {
        return leaderNode != null;
    }

    public void raftRole(RaftRole role) {
        RaftRole oldRaftRole = raftRole;
        raftRole = role;

        logger.info("Transform raftRole from [{}] to [{}]", oldRaftRole, raftRole);
    }

    public ServerNode leaderNode() {
        return leaderNode;
    }

    public void leaderNode(ServerNode node) {
        leaderNode = node;
    }
}
