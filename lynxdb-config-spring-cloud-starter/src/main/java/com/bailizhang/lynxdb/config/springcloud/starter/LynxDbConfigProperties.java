package com.bailizhang.lynxdb.config.springcloud.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Objects;

@ConfigurationProperties("spring.cloud.config")
public class LynxDbConfigProperties {
    private static final String DEFAULT_PROFILE_NAME = "default";

    private String profile;

    public LynxDbConfigProperties() {
    }

    public String profile() {
        return Objects.isNull(profile) ? DEFAULT_PROFILE_NAME : profile;
    }
}
