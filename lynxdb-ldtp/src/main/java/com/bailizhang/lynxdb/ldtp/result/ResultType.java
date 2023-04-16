package com.bailizhang.lynxdb.ldtp.result;

public interface ResultType {
    byte LDTP = (byte) 0x01;
    byte RAFT_RPC = (byte) 0x02;
    byte REDIRECT = (byte) 0x03;
}
