package com.bailizhang.lynxdb.raft.log.entry;

public interface LogEntryType {
    byte KV_STORE = (byte) 0x01;
    byte TABLE = (byte) 0x02;
    byte CLUSTER_MEMBER = (byte) 0x03;
}
