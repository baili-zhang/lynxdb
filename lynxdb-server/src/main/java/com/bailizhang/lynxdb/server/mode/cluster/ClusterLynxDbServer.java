package com.bailizhang.lynxdb.server.mode.cluster;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.server.RaftServerHandler;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.io.IOException;

public class ClusterLynxDbServer implements LynxDbServer {
    private final RaftServer raftServer;
    private final RaftClient raftClient;

    public ClusterLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        raftClient = new RaftClient();
        raftServer = new RaftServer(current, raftClient);

        RaftState raftState = RaftState.getInstance();
        raftState.raftClient(raftClient);
        raftState.raftServer(raftServer);

        raftServer.setHandler(new RaftServerHandler(raftServer));
        raftServer.setClientHandler(new RaftClientHandler());
    }

    @Override
    public void run() {
        Executor.start(raftServer);
        Executor.start(raftClient);
    }
}
