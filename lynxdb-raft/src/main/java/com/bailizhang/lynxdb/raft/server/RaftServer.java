package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.core.RaftRpcHandler;
import com.bailizhang.lynxdb.raft.core.RaftTimeWheel;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer extends SocketServer {

    public RaftServer(ServerNode current) throws IOException {
        super(new SocketServerConfig(current.port()));
    }

    @Override
    protected void doBeforeExecute() {
        RaftRpcHandler raftRpcHandler = new RaftRpcHandler();
        setHandler(new RaftServerHandler(this, raftRpcHandler));

        RaftTimeWheel timeWheel = RaftTimeWheel.timeWheel();
        timeWheel.start();

        RaftClient raftClient = RaftClient.client();
        Executor.start(raftClient);

        // 注册定时器任务
        timeWheel.registerElectionTimeoutTask();

        super.doBeforeExecute();
    }
}
