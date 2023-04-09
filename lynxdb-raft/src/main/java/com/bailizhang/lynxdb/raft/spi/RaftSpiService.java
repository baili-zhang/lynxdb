package com.bailizhang.lynxdb.raft.spi;

import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

public class RaftSpiService {
    private static final int MIN_MEMBERS_SIZE = 3;

    private static final RaftSpiService service = new RaftSpiService();

    private final StateMachine stateMachine;
    private final RaftConfiguration raftConfig;

    private RaftSpiService() {
        stateMachine = serviceLoad(StateMachine.class);
        raftConfig = serviceLoad(RaftConfiguration.class);

        List<ServerNode> initClusterMembers = stateMachine.clusterMembers();
        if(initClusterMembers.isEmpty()) {
            initClusterMembers = raftConfig.initClusterMembers();
        }

        if(initClusterMembers.size() < MIN_MEMBERS_SIZE) {
            throw new RuntimeException();
        }
    }

    public static RaftConfiguration raftConfig() {
        return service.raftConfig;
    }

    public static StateMachine stateMachine() {
        return service.stateMachine;
    }

    private  <T> T serviceLoad(Class<T> clazz) {
        ServiceLoader<T> serviceLoader = ServiceLoader.load(clazz);
        Optional<T> optional = serviceLoader.findFirst();

        if(optional.isEmpty()) {
            throw new RuntimeException("Can not find " + clazz.getSimpleName());
        }

        return optional.get();
    }
}
