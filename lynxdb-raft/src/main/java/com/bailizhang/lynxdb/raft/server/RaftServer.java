package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.client.RaftClient;
import com.bailizhang.lynxdb.raft.client.RaftClientHandler;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;

import java.io.IOException;
import java.util.Optional;
import java.util.ServiceLoader;

public class RaftServer extends SocketServer {
    private final RaftClient raftClient;
    private final StateMachine stateMachine;
    private final RaftConfiguration raftConfiguration;

    public RaftServer(ServerNode currentNode, RaftClient client)
            throws IOException {
        super(new SocketServerConfig(currentNode.port()));
        raftClient = client;

        stateMachine = serviceLoad(StateMachine.class);
        raftConfiguration = serviceLoad(RaftConfiguration.class);
    }

    public void setClientHandler(RaftClientHandler raftClientHandler) {
        raftClient.setHandler(raftClientHandler);
    }

    private <T> T serviceLoad(Class<T> clazz) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz);
        Optional<T> optional = serviceLoader.findFirst();

        if(optional.isEmpty()) {
            throw new RuntimeException("Can not find " + clazz.getSimpleName());
        }

        return optional.get();
    }
}
