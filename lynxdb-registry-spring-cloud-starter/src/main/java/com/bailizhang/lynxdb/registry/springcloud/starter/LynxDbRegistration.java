package com.bailizhang.lynxdb.registry.springcloud.starter;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.serviceregistry.Registration;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;

public class LynxDbRegistration implements Registration {
    public static final String COLUMN_FAMILY = "lynxdb-registry";
    public static final String INSTANCE_ID_TEMPLATE = "%s--%s:%s";

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
    public String getInstanceId() {
        return String.format(INSTANCE_ID_TEMPLATE, serviceId, host, port);
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
        return DefaultServiceInstance.getUri(this);
    }

    @Override
    public Map<String, String> getMetadata() {
        return null;
    }
}
