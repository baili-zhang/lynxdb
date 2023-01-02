package com.bailizhang.lynxdb.springboot.starter;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("com.bailizhang.lynxdb")
public class LynxDbProperties {
    private String host;
    private int port;
    private int messagePort;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getMessagePort() {
        return messagePort;
    }

    public void setMessagePort(int messagePort) {
        this.messagePort = messagePort;
    }
}
