package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public class LynxDbRaftConfiguration implements RaftConfiguration {
    private final String electionMode;
    private final ServerNode currentNode;
    private final String logDir;

    public LynxDbRaftConfiguration() {
        electionMode = Configuration.getInstance().electionMode();
        currentNode = Configuration.getInstance().currentNode();
        logDir = Configuration.getInstance().raftLogDir();
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
    public String logDir() {
        return logDir;
    }
}
