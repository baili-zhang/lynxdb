package zbl.moonlight.server.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MdtpMethod {
    byte CREATE_KV_STORE        = (byte) 0x01;
    byte DROP_KV_STORE          = (byte) 0x02;

    byte KV_SET                 = (byte) 0x03;
    byte KV_GET                 = (byte) 0x04;
    byte KV_DELETE              = (byte) 0x05;

    byte CREATE_TABLE           = (byte) 0x06;
    byte DROP_TABLE             = (byte) 0x07;
    byte CREATE_TABLE_COLUMN    = (byte) 0x08;
    byte DROP_TABLE_COLUMN      = (byte) 0x09;

    byte TABLE_GET              = (byte) 0x0a;
    byte TABLE_SET              = (byte) 0x0b;
    byte TABLE_DELETE           = (byte) 0x0c;

    byte SHOW_KVSTORE           = (byte) 0x0d;
    byte SHOW_TABLE             = (byte) 0x0e;

    byte value();
}
