package com.bailizhang.lynxdb.config.springcloud.starter;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.cloud.context.properties.ConfigurationPropertiesRebinder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class LynxDbConfigClient extends Shutdown implements Runnable {
    private final LynxDbTemplate lynxDbTemplate;
    private final ConfigurableApplicationContext applicationContext;

    public LynxDbConfigClient(ConfigurableApplicationContext applicationContext,
                              LynxDbTemplate lynxDbTemplate) {
        this.applicationContext = applicationContext;
        this.lynxDbTemplate = lynxDbTemplate;
    }

    @Override
    public void run() {
        while (isNotShutdown()) {
            AffectValue affectValue = lynxDbTemplate.onMessage();
            AffectKey affectKey = affectValue.affectKey();
            List<DbValue> dbValues = affectValue.dbValues();

            String name = G.I.toString(affectKey.key());

            ConfigurableEnvironment environment = applicationContext.getEnvironment();
            MutablePropertySources propertySources = environment.getPropertySources();
            MapPropertySource sources = (MapPropertySource)propertySources.get(name);

            Map<String, Object> map;

            if(sources == null) {
                map = new HashMap<>();
                sources = new MapPropertySource(name, map);
                propertySources.addLast(sources);
            } else {
                map = sources.getSource();
            }

            dbValues.forEach(dbValue -> map.put(
                    G.I.toString(dbValue.column()),
                    G.I.toString(dbValue.value())
            ));

            StringValuePostProcessor processor = applicationContext.getBean(StringValuePostProcessor.class);
            processor.reassign(environment.getPropertySources());
        }
    }
}
