package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.register.RegisterableSocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;

public class SingleLynxDbServer implements LynxDbServer {
    private final SocketServer server;
    private final SingleLdtpEngine engine;

    public SingleLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        server = new RegisterableSocketServer(new SocketServerConfig(current.port()));
        engine = new SingleLdtpEngine(server);

        SingleHandler handler = new SingleHandler(engine, server);
        server.setHandler(handler);
    }

    @Override
    public void run() {
        Executor.start(server);
        Executor.start(engine);
    }
}
