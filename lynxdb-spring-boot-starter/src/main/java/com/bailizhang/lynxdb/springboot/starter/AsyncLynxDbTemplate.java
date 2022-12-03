package com.bailizhang.lynxdb.springboot.starter;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;

public class AsyncLynxDbTemplate {
    protected final LynxDbClient client;
    protected final SelectionKey current;

    public AsyncLynxDbTemplate(LynxDbProperties properties) {
        client = new LynxDbClient();
        Executor.start(client);
        ServerNode server = new ServerNode(properties.getHost(), properties.getPort());

        try {
            current = client.connect(server);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }
}
