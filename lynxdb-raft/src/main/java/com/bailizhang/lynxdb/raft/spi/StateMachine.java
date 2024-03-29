package com.bailizhang.lynxdb.raft.spi;

import com.bailizhang.lynxdb.raft.core.ClientRequest;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

/**
 * Raft 定义的状态机的接口
 */
public interface StateMachine {
    void apply(List<ClientRequest> requests);

    List<ServerNode> clusterMembers();

    void addClusterMember(ServerNode current);

    void addClusterMembers(List<ServerNode> members);

    int currentTerm();

    void currentTerm(int term);

    ServerNode voteFor(int term);

    boolean voteForIfNull(int term, ServerNode node);
}
