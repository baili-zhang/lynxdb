package com.bailizhang.lynxdb.raft.log.entry;

public interface LogEntryMethod {
    byte SET = (byte) 0x01;
    byte DELETE = (byte) 0x02;
}
