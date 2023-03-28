package com.bailizhang.lynxdb.raft.spi;

import com.bailizhang.lynxdb.socket.client.ServerNode;

public interface RaftConfiguration {
    String electionMode();

    ServerNode currentNode();

    String logsDir();

    String metaDir();
}
