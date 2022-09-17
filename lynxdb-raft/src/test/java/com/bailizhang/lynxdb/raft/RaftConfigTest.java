package com.bailizhang.lynxdb.raft;

import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

public class RaftConfigTest extends RaftConfiguration {
    @Override
    public String electionMode() {
        return LEADER;
    }

    @Override
    public ServerNode currentNode() {
        return new ServerNode("127.0.0.1", 7820);
    }

    @Override
    public String logDir() {
        return System.getProperty("user.dir") + "/logs/raft";
    }
}
