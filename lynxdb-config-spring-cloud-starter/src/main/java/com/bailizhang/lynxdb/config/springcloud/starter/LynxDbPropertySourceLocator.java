package com.bailizhang.lynxdb.config.springcloud.starter;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.bootstrap.config.PropertySourceLocator;
import org.springframework.core.env.CompositePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.PropertySource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class LynxDbPropertySourceLocator implements PropertySourceLocator {
    private static final String PROPERTY_SOURCE_NAME = "lynxdbConfigProperties";

    @Autowired
    private LynxDbTemplate lynxDbTemplate;

    @Value("${spring.application.name}")
    private String serviceId;

    private final LynxDbConfigProperties lynxDbConfigProperties;

    public LynxDbPropertySourceLocator(LynxDbConfigProperties properties) {
        this.lynxDbConfigProperties = properties;
    }

    @Override
    public PropertySource<?> locate(Environment environment) {
        List<String> profiles = new ArrayList<>();

        String profile = lynxDbConfigProperties.profile();

        String[] activeProfiles = environment.getActiveProfiles();
        if(activeProfiles.length != 0) {
            profiles.addAll(Arrays.asList(activeProfiles));
            profiles.add(profile);
        } else {
            profiles.add(profile);

            String[] defaultProfiles = environment.getDefaultProfiles();
            profiles.addAll(Arrays.asList(defaultProfiles));
        }

        if(serviceId == null) {
            throw new RuntimeException();
        }

        byte[] columnFamily = G.I.toBytes(serviceId);
        List<byte[]> keys = profiles.stream().map(G.I::toBytes).toList();

        CompositePropertySource propertySource = new CompositePropertySource(PROPERTY_SOURCE_NAME);
        keys.forEach(key -> {
            List<DbValue> dbValues = lynxDbTemplate.find(key, columnFamily);

            HashMap<String, Object> map = new HashMap<>();
            String name = G.I.toString(key);

            MapPropertySource mapPropertySource = new MapPropertySource(name, map);

            dbValues.forEach(dbValue -> {
                String sourceKey = G.I.toString(dbValue.column());
                String sourceValue = G.I.toString(dbValue.value());

                map.put(sourceKey, sourceValue);
            });

            propertySource.addPropertySource(mapPropertySource);
        });

        return propertySource;
    }
}
