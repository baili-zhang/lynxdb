package com.bailizhang.lynxdb.springcloud.starter;

import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.serviceregistry.ServiceRegistry;

import java.util.List;

public class LynxDbServiceRegistry implements ServiceRegistry<LynxDbRegistration> {
    public static final String KVSTORE_NAME = "lynxdb-registry";

    private static final String URL_TEMPLATE = "%s:%d";

    @Autowired
    private LynxDbTemplate lynxDbTemplate;

    @Override
    public void register(LynxDbRegistration registration) {
        String serviceId = registration.getServiceId();

        String host = registration.getHost();
        int port = registration.getPort();
        String url = String.format(URL_TEMPLATE, host, port);

        lynxDbTemplate.kvValueListInsert(KVSTORE_NAME, serviceId, List.of(url));
    }

    @Override
    public void deregister(LynxDbRegistration registration) {
        String serviceId = registration.getServiceId();

        String host = registration.getHost();
        int port = registration.getPort();
        String url = String.format(URL_TEMPLATE, host, port);

        lynxDbTemplate.kvValueListRemove(KVSTORE_NAME, serviceId, List.of(url));
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
