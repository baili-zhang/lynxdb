package zbl.moonlight.starter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zbl.moonlight.core.common.Converter;
import zbl.moonlight.core.common.G;

import java.nio.charset.StandardCharsets;

@Configuration
@ConditionalOnClass(MoonlightTemplate.class)
@EnableConfigurationProperties(MoonlightProperties.class)
public class MoonlightAutoConfiguration {
    @Autowired
    private MoonlightProperties properties;

    @Bean
    @ConditionalOnMissingBean
    MoonlightTemplate moonlightTemplate (){
        G.I.converter(new Converter(StandardCharsets.UTF_8));
        return new MoonlightTemplate(properties);
    }
}
