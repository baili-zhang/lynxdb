package com.bailizhang.lynxdb.ldtp.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LdtpMethod {
    byte FIND_BY_KEY_CF_COLUMN          = (byte) 0x01;
    byte FIND_MULTI_COLUMNS             = (byte) 0x02;
    byte INSERT                         = (byte) 0x03;
    byte INSERT_MULTI_COLUMNS           = (byte) 0x04;
    byte DELETE                         = (byte) 0x05;
    byte DELETE_MULTI_COLUMNS           = (byte) 0x06;
    byte RANGE_NEXT                     = (byte) 0x07;
    byte RANGE_BEFORE                   = (byte) 0x08;
    byte EXIST_KEY                      = (byte) 0x09;

    byte CLUSTER_MEMBER_CHANGE          = (byte) 0x0a;
    byte CLUSTER_MEMBER_ADD             = (byte) 0x0b;
    byte JOIN                           = (byte) 0x0c;

    byte value();
}
