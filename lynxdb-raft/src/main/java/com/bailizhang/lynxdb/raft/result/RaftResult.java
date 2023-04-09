package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.socket.code.Result;

/**
 * TODO: 需要转移到 ldtp 包中
 */
public interface RaftResult extends Result {
    byte REQUEST_VOTE_RESULT        = (byte) 0x42;
    byte APPEND_ENTRIES_RESULT      = (byte) 0x43;
    byte INSTALL_SNAPSHOT_RESULT    = (byte) 0x44;
    byte LEADER_NOT_EXISTED_RESULT  = (byte) 0x45;
    byte JOIN_CLUSTER_RESULT        = (byte) 0x46;
}
