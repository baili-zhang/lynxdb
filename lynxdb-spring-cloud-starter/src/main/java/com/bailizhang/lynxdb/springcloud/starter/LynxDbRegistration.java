package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.cloud.client.serviceregistry.Registration;

import java.net.URI;
import java.util.Map;

public class LynxDbRegistration implements Registration {
    @Override
    public String getServiceId() {
        return null;
    }

    @Override
    public String getHost() {
        return null;
    }

    @Override
    public int getPort() {
        return 0;
    }

    @Override
    public boolean isSecure() {
        return false;
    }

    @Override
    public URI getUri() {
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        return null;
    }
}
