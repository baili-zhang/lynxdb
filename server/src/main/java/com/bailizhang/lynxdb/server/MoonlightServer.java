package com.bailizhang.lynxdb.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.server.RaftServerHandler;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final RaftServer raftServer;
    private final RaftClient raftClient;

    MoonlightServer() throws IOException {
        Configuration config = Configuration.getInstance();
        logger.info("Configuration: [{}]", config);

        G.I.converter(new Converter(config.charset()));

        ServerNode current = config.currentNode();

        raftClient = new RaftClient();
        raftServer = new RaftServer(current, raftClient);

        RaftState raftState = RaftState.getInstance();
        raftState.raftClient(raftClient);
        raftState.raftServer(raftServer);

        raftServer.setHandler(new RaftServerHandler(raftServer));
        raftServer.setClientHandler(new RaftClientHandler(raftServer, raftClient));
    }

    public void run() {
        Executor.start(raftServer);
        Executor.start(raftClient);
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
