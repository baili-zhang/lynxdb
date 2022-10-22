package com.bailizhang.lynxdb.raft.log.entry;

public interface LogEntryValid {
    byte VALID = (byte) 0x01;
    byte INVALID = (byte) 0x02;
}
