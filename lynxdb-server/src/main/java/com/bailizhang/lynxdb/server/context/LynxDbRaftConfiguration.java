package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public class LynxDbRaftConfiguration implements RaftConfiguration {
    private final String electionMode;
    private final ServerNode currentNode;
    private final String logsDir;
    private final String metaDir;

    public LynxDbRaftConfiguration() {
        electionMode = Configuration.getInstance().electionMode();
        currentNode = Configuration.getInstance().currentNode();
        logsDir = Configuration.getInstance().raftLogsDir();
        metaDir = Configuration.getInstance().raftMetaDir();
    }

    @Override
    public String electionMode() {
        return electionMode;
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
