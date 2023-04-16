package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.core.RaftTimeWheel;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SingleLynxDbServer implements LynxDbServer {
    private static final Logger logger = LoggerFactory.getLogger(SingleLynxDbServer.class);

    private final SocketServer server;
    private final LdtpEngineExecutor engineExecutor;

    public SingleLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        SocketServerConfig serverConfig = new SocketServerConfig(current.port());
        server = new SocketServer(serverConfig);
        server.startRegisterServer();

        engineExecutor = new LdtpEngineExecutor(server);

        SingleHandler handler = new SingleHandler(engineExecutor);
        server.setHandler(handler);
    }

    @Override
    public void run() {
        logger.info("Run LynxDB single server.");

        RaftTimeWheel raftTimeWheel = RaftTimeWheel.timeWheel();
        raftTimeWheel.start();

        Executor.start(server);
        Executor.start(engineExecutor);
    }
}
