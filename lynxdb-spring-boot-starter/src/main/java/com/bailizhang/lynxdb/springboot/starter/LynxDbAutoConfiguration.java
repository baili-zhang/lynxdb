package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.nio.charset.StandardCharsets;

@Configuration
@ComponentScan("com.bailizhang.lynxdb")
@ConditionalOnClass(LynxDbTemplate.class)
@EnableConfigurationProperties(LynxDbProperties.class)
public class LynxDbAutoConfiguration {
    @Autowired
    private LynxDbProperties properties;

    @Bean
    @ConditionalOnMissingBean
    LynxDbTemplate lynxDbTemplate(){
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        return new LynxDbTemplate(properties);
    }
}
