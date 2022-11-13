package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.serviceregistry.Registration;

import java.net.*;
import java.util.Map;

public class LynxDbRegistration implements Registration {

    private final String host;

    @Value("${spring.application.name}")
    private String serviceId;

    @Value("${server.port}")
    private int port;

    public LynxDbRegistration() {
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            host = inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    public LynxDbRegistration(String serviceId, String host, int port) {
        this.serviceId = serviceId;
        this.host = host;
        this.port = port;
    }

    @Override
    public String getServiceId() {
        return serviceId;
    }

    @Override
    public String getHost() {
        return host;
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
