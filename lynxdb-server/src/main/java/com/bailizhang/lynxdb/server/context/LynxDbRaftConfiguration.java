package com.bailizhang.lynxdb.server.context;

import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public class LynxDbRaftConfiguration implements RaftConfiguration {
    @Override
    public String electionMode() {
        return Configuration.getInstance().electionMode();
    }

    @Override
    public ServerNode currentNode() {
        return Configuration.getInstance().currentNode();
    }

    @Override
    public String logDir() {
        return Configuration.getInstance().raftLogDir();
    }
}
