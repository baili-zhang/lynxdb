package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

public class LynxDbRaftConfiguration implements RaftConfiguration {
    private final List<ServerNode> initClusterMembers;
    private final ServerNode currentNode;
    private final String logsDir;
    private final String metaDir;

    public LynxDbRaftConfiguration() {
        String members = Configuration.getInstance().initClusterMembers();

        initClusterMembers = ServerNode.parseNodeList(members);
        currentNode = Configuration.getInstance().currentNode();
        logsDir = Configuration.getInstance().raftLogsDir();
        metaDir = Configuration.getInstance().raftMetaDir();
    }

    @Override
    public List<ServerNode> initClusterMembers() {
        return initClusterMembers;
    }

    @Override
    public ServerNode currentNode() {
        return currentNode;
    }

    @Override
    public String logsDir() {
        return logsDir;
    }

    @Override
    public String metaDir() {
        return metaDir;
    }
}
