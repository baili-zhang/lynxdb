package zbl.moonlight.core.raft.client;

import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static zbl.moonlight.core.raft.response.RaftResponse.*;

public record RaftClientHandler(RaftState raftState, SocketServer raftServer)
        implements SocketClientHandler {
    @Override
    public void handleResponse(SocketResponse response) {
        ByteBuffer buffer = ByteBuffer.wrap(response.data());
        byte status = buffer.get();

        switch (status) {
            case REQUEST_VOTE_SUCCESS, REQUEST_VOTE_FAILURE,
                    APPEND_ENTRIES_SUCCESS, APPEND_ENTRIES_FAILURE -> {
                int term = buffer.getInt();
                int len = buffer.getInt();
                byte[] host = new byte[len];
                buffer.get(host);
                int port = buffer.getInt();
                ServerNode node = new ServerNode(new String(host), port);
                handleRaftRpcResponse(status, term, node, buffer);
            }

            case CLIENT_REQUEST_SUCCESS, CLIENT_REQUEST_FAILURE -> {
                int len = buffer.limit() - buffer.position();
                byte[] commandResult = new byte[len];
                buffer.get(commandResult);
                raftServer.offer(new SocketResponse((SelectionKey) response.attachment(),
                        commandResult, null));
            }
        }
    }

    @Override
    public void handleAfterLatchAwait() {
        /* 如果心跳超时，则需要发送心跳包 */
        if(raftState.isHeartbeatTimeout()) {

        }
        /* 如果选举超时，则需要升级为 Candidate，并向其他节点发送 RequestVote 请求 */
        if(raftState.isElectionTimeout()) {

        }
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) {
        switch (status) {
            case REQUEST_VOTE_SUCCESS -> {
                raftState.setVotedNodeAndCheck(node);
            }
            case APPEND_ENTRIES_SUCCESS -> {
                int matchedIndex = buffer.getInt();
                raftState.nextIndex().put(node, matchedIndex + 1);
                raftState.matchedIndex().put(node, matchedIndex);
                raftState.checkCommitIndex();
            }
            case APPEND_ENTRIES_FAILURE -> {
                int nextIndex = raftState.nextIndex().get(node);
                raftState.nextIndex().put(node, nextIndex - 1);
            }
        }
    }
}
