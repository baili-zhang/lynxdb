package com.bailizhang.lynxdb.springcloud.starter.discovery;

import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass({
        LynxDbTemplate.class,
        LynxDbServiceRegistry.class,
        LynxDbAutoServiceRegistration.class
})
public class LynxDbRegistryAutoConfiguration {

    @Bean
    LynxDbRegistration lynxDbRegistration() {
        return new LynxDbRegistration();
    }

    @Bean
    LynxDbServiceRegistry lynxDbServiceRegistry() {
        return new LynxDbServiceRegistry();
    }

    @Bean
    LynxDbAutoServiceRegistration lynxDbAutoServiceRegistration(
            LynxDbServiceRegistry lynxDbServiceRegistry
    ) {
        return new LynxDbAutoServiceRegistration(
                lynxDbServiceRegistry,
                new AutoServiceRegistrationProperties()
        );
    }

    @Bean
    LynxDbDiscoveryClient lynxDbDiscoveryClient() {
        return new LynxDbDiscoveryClient();
    }
}
