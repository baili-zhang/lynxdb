package zbl.moonlight.raft.client;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.raft.state.RaftRole;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.socket.client.SocketClient;

import java.io.IOException;

public class RaftClient extends SocketClient {
    public RaftClient() throws IOException {
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
