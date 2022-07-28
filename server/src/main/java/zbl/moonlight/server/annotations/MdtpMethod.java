package zbl.moonlight.server.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface MdtpMethod {
    byte KV_SINGLE_GET = (byte) 0x01;
    byte KV_SINGLE_SET = (byte) 0x02;
    byte KV_SINGLE_DELETE = (byte) 0x03;

    byte value();
}
