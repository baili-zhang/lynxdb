package com.bailizhang.lynxdb.ldtp.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LdtpMethod {
    byte FIND_BY_KEY_CF_COLUMN          = (byte) 0x01;
    byte FIND_BY_KEY_CF                 = (byte) 0x02;
    byte INSERT                         = (byte) 0x03;
    byte INSERT_MULTI_COLUMN            = (byte) 0x04;
    byte DELETE                         = (byte) 0x05;
    byte DELETE_MULTI_COLUMN            = (byte) 0x06;

    byte value();
}
