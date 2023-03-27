package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
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
        RaftRpcHandler raftRpcHandler = new RaftRpcHandler();
        setHandler(new RaftServerHandler(this, raftRpcHandler));

        SocketTimeWheel socketTimeWheel = SocketTimeWheel.timeWheel();
        socketTimeWheel.start();

        RaftClient raftClient = RaftClient.client();
        Executor.start(raftClient);

        // 连接集群的节点
        raftRpcHandler.connectClusterMembers();
        // 注册定时器任务
        raftRpcHandler.registerTimeoutTasks();

        super.doBeforeExecute();
    }
}
