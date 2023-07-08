package com.bailizhang.lynxdb.ldtp.request;

public interface RequestType {
    byte LDTP_METHOD        = (byte) 0x01;
    byte RAFT_RPC           = (byte) 0x02;
    byte FLIGHT_RECORDER    = (byte) 0x03;
}
