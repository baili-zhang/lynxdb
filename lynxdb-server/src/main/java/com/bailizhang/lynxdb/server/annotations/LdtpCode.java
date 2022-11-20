package com.bailizhang.lynxdb.server.annotations;

public @interface LdtpCode {
    byte SUCCESS = (byte) 0x01;
    byte SUCCESS_WITH_LIST = (byte) 0x02;
    byte SUCCESS_WITH_KV_PAIRS = (byte) 0x03;
    byte SUCCESS_WITH_TABLE = (byte) 0x04;

    byte INVALID_ARGUMENT = (byte) 0x70;
}
