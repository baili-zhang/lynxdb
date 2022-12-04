package com.bailizhang.lynxdb.server.annotations;

public @interface LdtpCode {
    byte VOID           = (byte) 0x01;
    byte NULL           = (byte) 0x02;
    byte BYTE_ARRAY     = (byte) 0x03;
    byte DB_VALUE_LIST  = (byte) 0x04;
}
