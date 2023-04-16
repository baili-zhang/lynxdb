package com.bailizhang.lynxdb.raft.spi;

import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

public interface RaftConfiguration {
    List<ServerNode> initClusterMembers();

    ServerNode currentNode();

    String logsDir();

    String metaDir();
}
