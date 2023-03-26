package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.core.RaftRpcHandler;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;

import java.io.IOException;

public class RaftServer extends SocketServer {
    public RaftServer(ServerNode current) throws IOException {
        super(new SocketServerConfig(current.port()));
    }

    @Override
    protected void doBeforeExecute() {
        RaftClient raftClient = new RaftClient();
        RaftRpcHandler raftRpcHandler = new RaftRpcHandler(raftClient);

        setHandler(new RaftServerHandler(this, raftRpcHandler));
        raftClient.setHandler(new RaftClientHandler(raftRpcHandler));

        SocketTimeWheel socketTimeWheel = SocketTimeWheel.timeWheel();
        socketTimeWheel.start();

        Executor.start(raftClient);

        raftRpcHandler.registerTimeoutTasks();

        super.doBeforeExecute();
    }
}
