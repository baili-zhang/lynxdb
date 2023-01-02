package com.bailizhang.lynxdb.registry.springcloud.starter;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.List;
import java.util.Objects;

public class LynxDbDiscoveryClient implements DiscoveryClient {
    @Autowired
    private LynxDbTemplate lynxDbTemplate;

    @Override
    public String description() {
        return null;
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceId) {
        List<DbValue> dbValues = lynxDbTemplate.find(
                G.I.toBytes(serviceId),
                G.I.toBytes(LynxDbRegistration.COLUMN_FAMILY)
        );

        return dbValues.stream()
                .map(dbValue -> {
                    byte[] value = dbValue.value();
                    String url = G.I.toString(value);
                    String[] arr = url.split(":");

                    if(arr.length != 2) {
                        return null;
                    }

                    try {
                        return (ServiceInstance) new LynxDbRegistration(
                                serviceId,
                                arr[0],
                                Integer.parseInt(arr[1])
                        );
                    } catch (NumberFormatException o_0) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .toList();
    }

    @Override
    public List<String> getServices() {
        return null;
    }
}
