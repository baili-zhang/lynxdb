package com.bailizhang.lynxdb.server.mode.cluster;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.ldtp.LdtpStateMachine;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ClusterLynxDbServer implements LynxDbServer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLynxDbServer.class);

    private final RaftServer raftServer;
    private final LdtpEngineExecutor engineExecutor;

    public ClusterLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        raftServer = new RaftServer(current);
        engineExecutor = new LdtpEngineExecutor(raftServer);

        LdtpStateMachine.engineExecutor(engineExecutor);
        LdtpStateMachine.raftServer(raftServer);
    }

    @Override
    public void run() {
        logger.info("Run LynxDB cluster server.");

        Executor.start(raftServer);
        Executor.start(engineExecutor);
    }
}
