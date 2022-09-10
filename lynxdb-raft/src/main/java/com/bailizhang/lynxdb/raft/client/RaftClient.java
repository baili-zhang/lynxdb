package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.raft.state.RaftRole;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;

public class RaftClient extends SocketClient {
    public RaftClient() {
    }

    @Override
    public void broadcast(BytesConvertible message) {
        if(connectedNodes().isEmpty()) {
            RaftState.getInstance().raftRole(RaftRole.LEADER);
            return;
        }
        super.broadcast(message);
    }
}
