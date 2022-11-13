package com.bailizhang.lynxdb.springcloud.starter;

import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.List;

public class LynxDbDiscoveryClient implements DiscoveryClient {
    @Autowired
    private LynxDbTemplate lynxDbTemplate;

    @Override
    public String description() {
        return null;
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        List<String> instances = lynxDbTemplate.kvValueListGet(
                LynxDbServiceRegistry.KVSTORE_NAME,
                serviceId
        );

        return instances.stream().map(instance -> {
            String[] arr = instance.split(":");
            return (ServiceInstance) new LynxDbRegistration(
                    serviceId,
                    arr[0],
                    Integer.parseInt(arr[1])
            );
        }).toList();
    }

    @Override
    public List<String> getServices() {
        return null;
    }
}
