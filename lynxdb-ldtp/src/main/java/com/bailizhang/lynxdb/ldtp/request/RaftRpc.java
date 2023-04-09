package com.bailizhang.lynxdb.ldtp.request;

public interface RaftRpc {
    byte PRE_VOTE               = (byte) 0x01;
    byte REQUEST_VOTE           = (byte) 0x02;
    byte APPEND_ENTRIES         = (byte) 0x03;
    byte INSTALL_SNAPSHOT       = (byte) 0x04;
    byte JOIN_CLUSTER           = (byte) 0x05;
    byte LEAVE_CLUSTER          = (byte) 0x06;
}
