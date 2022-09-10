package com.bailizhang.lynxdb.raft.request;


public interface RaftRequest {
    byte CLIENT_REQUEST = (byte) 0x01;
    byte REQUEST_VOTE = (byte) 0x02;
    byte APPEND_ENTRIES = (byte) 0x03;
    byte INSTALL_SNAPSHOT = (byte) 0x04;
}
