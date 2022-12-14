package com.bailizhang.lynxdb.springcloud.starter.config;

import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({
        LynxDbTemplate.class
})
public class LynxDbConfigAutoConfiguration {
    @Bean
    @ConditionalOnMissingBean
    public LynxDbConfigProperties lynxDbConfigProperties() {
        return new LynxDbConfigProperties();
    }

    @Bean
    @ConditionalOnMissingBean
    public LynxDbPropertySourceLocator lynxDbPropertySourceLocator(
            LynxDbConfigProperties lynxDbConfigProperties
    ) {
        return new LynxDbPropertySourceLocator(lynxDbConfigProperties);
    }
}
