package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.cloud.client.serviceregistry.AbstractAutoServiceRegistration;
import org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

public class LynxDbAutoServiceRegistration
        extends AbstractAutoServiceRegistration<LynxDbRegistration> {
    protected LynxDbAutoServiceRegistration(ServiceRegistry<LynxDbRegistration> serviceRegistry, AutoServiceRegistrationProperties properties) {
        super(serviceRegistry, properties);
    }

    @Override
    protected Object getConfiguration() {
        return null;
    }

    @Override
    protected boolean isEnabled() {
        return false;
    }

    @Override
    protected LynxDbRegistration getRegistration() {
        return null;
    }

    @Override
    protected LynxDbRegistration getManagementRegistration() {
        return null;
    }
}
