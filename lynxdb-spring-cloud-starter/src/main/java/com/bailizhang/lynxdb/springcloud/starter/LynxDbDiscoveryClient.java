package com.bailizhang.lynxdb.springcloud.starter;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.List;

public class LynxDbDiscoveryClient implements DiscoveryClient {
    @Override
    public String description() {
        return null;
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        return null;
    }

    @Override
    public List<String> getServices() {
        return null;
    }
}
