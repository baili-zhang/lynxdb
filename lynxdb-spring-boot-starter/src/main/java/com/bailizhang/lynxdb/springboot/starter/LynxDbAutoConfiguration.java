package com.bailizhang.lynxdb.springboot.starter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;

import java.nio.charset.StandardCharsets;

@Configuration
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
