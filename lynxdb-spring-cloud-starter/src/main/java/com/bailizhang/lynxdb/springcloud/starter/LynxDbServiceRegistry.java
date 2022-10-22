package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

public class LynxDbServiceRegistry implements ServiceRegistry<LynxDbRegistration> {
    @Override
    public void register(LynxDbRegistration registration) {

    }

    @Override
    public void deregister(LynxDbRegistration registration) {

    }

    @Override
    public void close() {

    }

    @Override
    public void setStatus(LynxDbRegistration registration, String status) {

    }

    @Override
    public <T> T getStatus(LynxDbRegistration registration) {
        return null;
    }
}
