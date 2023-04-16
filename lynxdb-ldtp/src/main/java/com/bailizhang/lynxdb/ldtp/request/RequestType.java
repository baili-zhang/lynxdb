package com.bailizhang.lynxdb.ldtp.request;

public interface RequestType {
    /**
     * 存储数据操作请求
     */
    byte LDTP_METHOD        = (byte) 0x01;
    byte RAFT_RPC           = (byte) 0x02;
    byte KEY_REGISTER       = (byte) 0x03;
    byte KEY_TIMEOUT        = (byte) 0x04;
}
