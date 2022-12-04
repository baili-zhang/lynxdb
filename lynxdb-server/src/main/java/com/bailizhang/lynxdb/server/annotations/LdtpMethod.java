package com.bailizhang.lynxdb.server.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface LdtpMethod {
    byte FIND_BY_KEY_CF_COLUMN  = (byte) 0x01;
    byte FIND_BY_KEY_CF         = (byte) 0x02;
    byte INSERT                 = (byte) 0x03;
    byte DELETE                 = (byte) 0x04;

    byte value();
}
