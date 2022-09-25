package com.bailizhang.lynxdb.raft.state;

public class RaftState extends StateMachineRaftState {
    public static final byte CLIENT_COMMAND = (byte) 0x01;
    public static final byte MEMBER_CHANGE = (byte) 0x02;

    private static final RaftState RAFT_STATE = new RaftState();

    public static RaftState getInstance() {
        return RAFT_STATE;
    }

    private RaftState() {
    }
}
