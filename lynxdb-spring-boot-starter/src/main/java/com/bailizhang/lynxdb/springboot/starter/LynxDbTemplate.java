package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.client.LynxDbClient;

public class LynxDbTemplate extends LynxDbClient implements AutoCloseable {
    public LynxDbTemplate(LynxDbProperties properties) {
        String host = properties.getHost();
        int port = properties.getPort();

        super.start();
        super.connect(host, port);
    }

    @Override
    public void connect(String host, int port) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {

    }
}
