package com.bailizhang.lynxdb.raft.common;

import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

/**
 * Raft 定义的状态机的接口
 */
public interface StateMachine {
    void apply(byte[] data);

    List<ServerNode> clusterMembers();

    void addClusterMember(ServerNode current);

    int currentTerm();

    void currentTerm(int term);
}
