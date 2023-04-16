package com.bailizhang.lynxdb.ldtp.result;

public interface RaftRpcResult {
    byte PRE_VOTE_RESULT            = (byte) 0x01;
    byte REQUEST_VOTE_RESULT        = (byte) 0x02;
    byte APPEND_ENTRIES_RESULT      = (byte) 0x03;
    byte INSTALL_SNAPSHOT_RESULT    = (byte) 0x04;
    byte LEADER_NOT_EXISTED_RESULT  = (byte) 0x05;
    byte JOIN_CLUSTER_RESULT        = (byte) 0x06;
}
