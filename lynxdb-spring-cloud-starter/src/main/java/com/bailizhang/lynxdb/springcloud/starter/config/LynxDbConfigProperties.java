package com.bailizhang.lynxdb.springcloud.starter.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Objects;

@ConfigurationProperties("com.bailizhang.lynxdb.config")
public class LynxDbConfigProperties {
    private static final String DEFAULT_PROFILE_NAME = "default";

    private String profile;

    public LynxDbConfigProperties() {
    }

    public String profile() {
        return Objects.isNull(profile) ? DEFAULT_PROFILE_NAME : profile;
    }
}
