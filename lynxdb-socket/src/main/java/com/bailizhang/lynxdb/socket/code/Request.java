package com.bailizhang.lynxdb.socket.code;

public interface Request {
    byte CLIENT_REQUEST     = (byte) 0x01;
    byte REGISTER_KEY       = (byte) 0x02;
    byte DEREGISTER_KEY     = (byte) 0x03;
    byte SET_TIMEOUT_KEY    = (byte) 0x04;
    byte REMOVE_TIMEOUT_KEY = (byte) 0x05;
}
