package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.core.common.BytesConvertible;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;
import com.bailizhang.lynxdb.raft.common.RaftRole;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.socket.client.SocketClient;

public class RaftClient extends SocketClient {
    public RaftClient() {
    }

    @Override
    public void broadcast(BytesConvertible message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void broadcast(BytesListConvertible message) {
        if(connectedNodes().isEmpty()) {
            RaftState.getInstance().raftRole(RaftRole.LEADER);
            return;
        }
        super.broadcast(message.toBytesList());
    }
}
