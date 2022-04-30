package zbl.moonlight.core.raft.client;

import zbl.moonlight.core.raft.response.RaftResponse;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.response.SocketResponse;

import java.nio.ByteBuffer;

public record RaftClientHandler(RaftState raftState) implements SocketClientHandler {
    public void handleResponse(SocketResponse response) {
        ByteBuffer buffer = ByteBuffer.wrap(response.data());
        byte status = buffer.get();

        switch (status) {
            case RaftResponse.REQUEST_VOTE_SUCCESS -> {
                int term = buffer.getInt();
                if(term < raftState.currentTerm()) {
                    return;
                }
                int len = buffer.getInt();
                byte[] host = new byte[len];
                buffer.get(host);
                int port = buffer.getInt();
                raftState.setVotedNodeAndCheck(new ServerNode(new String(host), port));
            }

            case RaftResponse.REQUEST_VOTE_FAILURE -> {
                // 请求投票失败不需要做处理
            }

            case RaftResponse.APPEND_ENTRIES_SUCCESS -> {

            }

            case RaftResponse.APPEND_ENTRIES_FAILURE -> {

            }
        }
    }
}
