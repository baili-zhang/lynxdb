package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleLynxDbServer implements LynxDbServer {
    private static final Logger logger = LoggerFactory.getLogger(SingleLynxDbServer.class);

    private final SocketServer server;
    private final SingleLdtpEngine engine;

    private final LynxDbTimeWheel timeWheel;

    public SingleLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        SocketServerConfig serverConfig = new SocketServerConfig(current.port());
        server = new SocketServer(serverConfig);
        server.startRegisterServer();

        timeWheel = new LynxDbTimeWheel();
        engine = new SingleLdtpEngine(server, timeWheel);

        SingleHandler handler = new SingleHandler(engine);
        server.setHandler(handler);
    }

    @Override
    public void run() {
        logger.info("Run LynxDB single server.");

        Executor.start(server);
        Executor.start(engine);
        Executor.startRunnable(timeWheel);
    }
}
