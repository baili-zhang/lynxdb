package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.client.LynxDbClient;

import java.nio.channels.SelectionKey;

public class LynxDbTemplate extends LynxDbClient implements AutoCloseable {

    public LynxDbTemplate(LynxDbProperties properties) {
        String host = properties.getHost();
        int port = properties.getPort();
        int messagePort = properties.getMessagePort();

        super.start();
        super.connect(host, port);
        super.registerConnect(host, messagePort);
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
    public SelectionKey current() {
        throw new UnsupportedOperationException();
    }


    @Override
    public void close() {

    }
}
