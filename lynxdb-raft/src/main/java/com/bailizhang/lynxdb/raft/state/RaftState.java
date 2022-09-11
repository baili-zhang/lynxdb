package com.bailizhang.lynxdb.raft.state;

public class RaftState extends StateMachineRaftState {
    private static final RaftState RAFT_STATE = new RaftState();

    public static RaftState getInstance() {
        return RAFT_STATE;
    }

    private RaftState() {
    }
}
