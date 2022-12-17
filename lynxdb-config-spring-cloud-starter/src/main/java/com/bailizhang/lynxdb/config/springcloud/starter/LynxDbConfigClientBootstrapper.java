package com.bailizhang.lynxdb.config.springcloud.starter;

import org.springframework.boot.BootstrapRegistry;
import org.springframework.boot.BootstrapRegistryInitializer;

public class LynxDbConfigClientBootstrapper implements BootstrapRegistryInitializer {
    @Override
    public void initialize(BootstrapRegistry registry) {
        System.out.println(registry);
    }
}
