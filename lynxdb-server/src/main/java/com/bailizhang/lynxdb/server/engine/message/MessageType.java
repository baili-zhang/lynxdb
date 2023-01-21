package com.bailizhang.lynxdb.server.engine.message;

public interface MessageType {
    byte REGISTER = (byte) 0x01;
    byte TIMEOUT = (byte) 0x02;
}
