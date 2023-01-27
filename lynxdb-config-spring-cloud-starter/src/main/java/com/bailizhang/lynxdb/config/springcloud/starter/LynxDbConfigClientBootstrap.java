package com.bailizhang.lynxdb.config.springcloud.starter;

import com.bailizhang.lynxdb.client.message.MessageHandler;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.boot.ConfigurableBootstrapContext;
import org.springframework.boot.SpringApplicationRunListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Component
public class LynxDbConfigClientBootstrap implements SpringApplicationRunListener {
    private final HashSet<String> profiles = new HashSet<>();

    private String serviceId;

    @Override
    public void environmentPrepared(ConfigurableBootstrapContext bootstrapContext,
                                    ConfigurableEnvironment environment) {
        serviceId = environment.getProperty("spring.application.name");
        String profile = environment.getProperty("spring.cloud.config.profile");

        String[] activeProfiles = environment.getActiveProfiles();
        if(activeProfiles.length != 0) {
            profiles.addAll(Arrays.asList(activeProfiles));
        } else {
            String[] defaultProfiles = environment.getDefaultProfiles();
            profiles.addAll(Arrays.asList(defaultProfiles));
        }

        profiles.add(profile);
    }

    @Override
    public void started(ConfigurableApplicationContext context, Duration timeTaken) {
        LynxDbTemplate lynxDbTemplate = context.getBean(LynxDbTemplate.class);

        if(serviceId == null) {
            throw new RuntimeException();
        }

        byte[] columnFamily = G.I.toBytes(serviceId);
        List<byte[]> keys = profiles.stream().map(G.I::toBytes).toList();

        MessageHandler messageHandler = new LynxDbConfigMessageHandler(context);

        keys.forEach(key -> {
            MessageKey messageKey = new MessageKey(key, columnFamily);

            lynxDbTemplate.registerAffectHandler(messageKey, messageHandler);
            lynxDbTemplate.register(key, columnFamily);
        });

    }
}
