package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.serviceregistry.Registration;

import java.net.*;
import java.util.Map;

public class LynxDbRegistration implements Registration {

    @Value("${spring.application.name}")
    private String serviceId;

    @Value("${server.port}")
    private int port;

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public String getHost() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getPort() {
        return port;
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
