package com.bailizhang.lynxdb.config.springcloud.starter;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.springboot.starter.LynxDbTemplate;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
            // AffectValue affectValue = lynxDbTemplate.onMessage();

            // TODO
            AffectValue affectValue = new AffectValue(null, null);

            MessageKey messageKey = affectValue.messageKey();
            List<DbValue> dbValues = affectValue.dbValues();

            String name = G.I.toString(messageKey.key());

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
