package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;

import java.io.IOException;
import java.util.Optional;
import java.util.ServiceLoader;

public class RaftServer extends SocketServer {
    private final StateMachine stateMachine;
    private final RaftConfiguration raftConfiguration;

    public RaftServer(ServerNode current) throws IOException {
        super(new SocketServerConfig(current.port()));

        stateMachine = serviceLoad(StateMachine.class);
        raftConfiguration = serviceLoad(RaftConfiguration.class);
    }

    public StateMachine stateMachine() {
        return stateMachine;
    }

    public RaftConfiguration raftConfiguration() {
        return raftConfiguration;
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
