package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.state.RaftState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer extends SocketServer {
    private static final Logger logger = LogManager.getLogger("RaftServer");

    private final RaftClient raftClient;

    public RaftServer(ServerNode currentNode, RaftClient client)
            throws IOException {
        super(new SocketServerConfig(currentNode.port()));
        raftClient = client;
    }

    @Override
    final protected void doBeforeExecute() {
        // 启动心跳超时计时器和选举超时计时器
        RaftState.getInstance().startTimeout();
    }

    public void setClientHandler(RaftClientHandler raftClientHandler) {
        raftClient.setHandler(raftClientHandler);
    }
}
