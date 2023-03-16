package com.bailizhang.lynxdb.server.mode.cluster;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.core.RaftRpcHandler;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.server.RaftServerHandler;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LynxDbServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;

import java.io.IOException;

public class ClusterLynxDbServer implements LynxDbServer {
    private final RaftServer raftServer;
    private final RaftClient raftClient;
    private final RaftRpcHandler raftRpcHandler;

    public ClusterLynxDbServer() throws IOException {
        Configuration config = Configuration.getInstance();
        ServerNode current = config.currentNode();

        raftRpcHandler = new RaftRpcHandler();

        raftClient = new RaftClient();
        raftServer = new RaftServer(current, raftClient);

        raftServer.setHandler(new RaftServerHandler(raftServer, raftRpcHandler));
        raftServer.setClientHandler(new RaftClientHandler(raftRpcHandler));
    }

    @Override
    public void run() {
        SocketTimeWheel socketTimeWheel = SocketTimeWheel.timeWheel();
        socketTimeWheel.start();

        Executor.start(raftServer);
        Executor.start(raftClient);

        raftRpcHandler.registerTimeoutTasks();
    }
}
