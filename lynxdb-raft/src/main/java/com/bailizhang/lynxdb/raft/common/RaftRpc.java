package com.bailizhang.lynxdb.raft.common;

public interface RaftRpc {
    byte CLIENT_COMMAND = (byte) 0x01;
    byte MEMBER_CHANGE = (byte) 0x02;
}
