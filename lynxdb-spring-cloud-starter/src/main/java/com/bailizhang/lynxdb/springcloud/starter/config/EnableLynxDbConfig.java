package com.bailizhang.lynxdb.springcloud.starter.config;

import com.bailizhang.lynxdb.springboot.starter.LynxDbAutoConfiguration;
import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import({
        LynxDbAutoConfiguration.class,
        LynxDbConfigAutoConfiguration.class
})
public @interface EnableLynxDbConfig {
}
