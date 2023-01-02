package com.bailizhang.lynxdb.registry.springcloud.starter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.serviceregistry.AbstractAutoServiceRegistration;
import org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

public class LynxDbAutoServiceRegistration
        extends AbstractAutoServiceRegistration<LynxDbRegistration> {

    @Autowired
    private LynxDbRegistration lynxDbRegistration;

    @Autowired
    private AutoServiceRegistrationProperties autoServiceRegistrationProperties;

    protected LynxDbAutoServiceRegistration(ServiceRegistry<LynxDbRegistration> serviceRegistry,
                                            AutoServiceRegistrationProperties properties) {
        super(serviceRegistry, properties);
    }

    @Override
    protected Object getConfiguration() {
        return null;
    }

    @Override
    protected boolean isEnabled() {
        return autoServiceRegistrationProperties.isEnabled();
    }

    @Override
    protected LynxDbRegistration getRegistration() {
        return lynxDbRegistration;
    }

    @Override
    protected LynxDbRegistration getManagementRegistration() {
        return null;
    }
}
