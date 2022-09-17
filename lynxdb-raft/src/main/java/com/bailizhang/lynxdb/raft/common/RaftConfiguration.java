package com.bailizhang.lynxdb.raft.common;

import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.Optional;
import java.util.ServiceLoader;

public abstract class RaftConfiguration {
    /**
     * 需要得到 leader 确认才能启动选举计时器
     */
    public static final String FOLLOWER = "follower";
    /**
     * 需要连接一半以上的节点后才能转换成 candidate
     */
    public static final String CANDIDATE = "candidate";
    /**
     * 选举计时器超时即可转换成 candidate
     */
    public static final String LEADER = "leader";

    private static final RaftConfiguration instance;

    static {
        ServiceLoader<RaftConfiguration> raftConfigurations = ServiceLoader.load(RaftConfiguration.class);
        Optional<RaftConfiguration> rcOptional = raftConfigurations.findFirst();

        if(rcOptional.isEmpty()) {
            throw new RuntimeException("Can not find RaftConfiguration.");
        }

        instance = rcOptional.get();
    }

    public static RaftConfiguration getInstance() {
        return instance;
    }

    protected RaftConfiguration() {

    }

    public abstract String electionMode();

    public abstract ServerNode currentNode();

    public abstract String logDir();
}
