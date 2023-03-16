package com.bailizhang.lynxdb.raft.request;


import com.bailizhang.lynxdb.socket.code.Request;

public interface RaftRequest extends Request {
    byte PRE_VOTE           = (byte) 0x42;
    byte REQUEST_VOTE       = (byte) 0x43;
    byte APPEND_ENTRIES     = (byte) 0x44;
    byte INSTALL_SNAPSHOT   = (byte) 0x45;
}
