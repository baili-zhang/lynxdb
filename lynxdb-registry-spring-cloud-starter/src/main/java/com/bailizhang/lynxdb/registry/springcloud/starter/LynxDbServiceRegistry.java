package com.bailizhang.lynxdb.registry.springcloud.starter;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

public class LynxDbServiceRegistry implements ServiceRegistry<LynxDbRegistration> {
    private static final String URL_TEMPLATE = "%s:%d";

    @Autowired
    private LynxDbTemplate lynxDbTemplate;

    @Override
    public void register(LynxDbRegistration registration) {
        String serviceId = registration.getServiceId();
        String instanceId = registration.getInstanceId();

        String host = registration.getHost();
        int port = registration.getPort();
        String url = String.format(URL_TEMPLATE, host, port);

        lynxDbTemplate.insert(
                G.I.toBytes(serviceId),
                G.I.toBytes(LynxDbRegistration.COLUMN_FAMILY),
                G.I.toBytes(instanceId),
                G.I.toBytes(url)
        );
    }

    @Override
    public void deregister(LynxDbRegistration registration) {
        String serviceId = registration.getServiceId();
        String instanceId = registration.getInstanceId();

        lynxDbTemplate.delete(
                G.I.toBytes(serviceId),
                G.I.toBytes(LynxDbRegistration.COLUMN_FAMILY),
                G.I.toBytes(instanceId)
        );
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
