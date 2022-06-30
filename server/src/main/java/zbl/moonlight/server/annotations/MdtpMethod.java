package zbl.moonlight.server.annotations;

import zbl.moonlight.server.mdtp.Method;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface MdtpMethod {
    Method value();
}
