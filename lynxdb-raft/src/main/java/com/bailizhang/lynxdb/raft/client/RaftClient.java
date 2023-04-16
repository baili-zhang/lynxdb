package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.raft.core.RaftRole;
import com.bailizhang.lynxdb.raft.core.RaftState;
import com.bailizhang.lynxdb.raft.core.RaftStateHolder;
import com.bailizhang.lynxdb.raft.request.JoinCluster;
import com.bailizhang.lynxdb.raft.request.JoinClusterArgs;
import com.bailizhang.lynxdb.raft.spi.RaftConfiguration;
import com.bailizhang.lynxdb.raft.spi.RaftSpiService;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.nio.channels.SelectionKey;

public class RaftClient extends SocketClient {
    private static final RaftClient client = new RaftClient();

    public RaftClient() {
    }

    public static RaftClient client() {
        return client;
    }

    @Override
    protected void doBeforeExecute() {
        setHandler(new RaftClientHandler());
        super.doBeforeExecute();
    }

    @Override
    public void broadcast(BytesConvertible message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void broadcast(BytesListConvertible message) {
        if(connectedNodes().isEmpty()) {
            RaftState raftState = RaftStateHolder.raftState();
            raftState.role().set(RaftRole.LEADER);
            return;
        }
        super.broadcast(message.toBytesList());
    }

    public void sendClusterMemberAdd(SelectionKey leader) {
        RaftConfiguration raftConfig = RaftSpiService.raftConfig();
        ServerNode current = raftConfig.currentNode();

        JoinClusterArgs args = new JoinClusterArgs(current);
        JoinCluster request = new JoinCluster(leader, args);

        send(request);
    }
}
