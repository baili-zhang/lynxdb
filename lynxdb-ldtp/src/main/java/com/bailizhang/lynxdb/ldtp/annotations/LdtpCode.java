package com.bailizhang.lynxdb.ldtp.annotations;

public @interface LdtpCode {
    byte VOID           = (byte) 0x01;
    byte TRUE           = (byte) 0x02;
    byte FALSE          = (byte) 0x03;
    byte NULL           = (byte) 0x04;
    byte BYTE_ARRAY     = (byte) 0x05;
    byte MULTI_COLUMNS  = (byte) 0x06;
    byte MULTI_KEYS     = (byte) 0x07;
}
