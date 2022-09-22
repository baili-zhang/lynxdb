package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.common.RaftRole;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;
import java.util.ServiceLoader;

public class BaseRaftState {
    private static final Logger logger = LogManager.getLogger("BaseRaftState");

    private static final String CURRENT_TERM = "current_term";

    protected static final StateMachine stateMachine;
    protected static final RaftConfiguration raftConfiguration;

    static {
        ServiceLoader<StateMachine> stateMachines = ServiceLoader.load(StateMachine.class);
        Optional<StateMachine> smOptional = stateMachines.findFirst();

        if(smOptional.isEmpty()) {
            throw new RuntimeException("Can not find StateMachine.");
        }

        stateMachine = smOptional.get();
        raftConfiguration = RaftConfiguration.getInstance();
    }

    protected volatile RaftRole raftRole = RaftRole.FOLLOWER;
    protected volatile int currentTerm;

    public void currentTerm(int term) {
        stateMachine.metaSet(CURRENT_TERM, BufferUtils.toBytes(term));
        currentTerm = term;
    }

    public synchronized void transformToCandidate() {
        raftRole(RaftRole.CANDIDATE);
        currentTerm(currentTerm ++);
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

    public void raftRole(RaftRole role) {
        RaftRole oldRaftRole = raftRole;
        raftRole = role;

        logger.info("Transform raftRole from [{}] to [{}]", oldRaftRole, raftRole);
    }
}
