package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer extends SocketServer {
    private final RaftClient raftClient;

    public RaftServer(ServerNode currentNode, RaftClient client)
            throws IOException {
        super(new SocketServerConfig(currentNode.port()));
        raftClient = client;
    }

    @Override
    protected final void doBeforeExecute() {
        // 启动心跳超时计时器和选举超时计时器
        RaftState.getInstance().startTimeout();
    }

    public void setClientHandler(RaftClientHandler raftClientHandler) {
        raftClient.setHandler(raftClientHandler);
    }
}
