package zbl.moonlight.core.raft.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static zbl.moonlight.core.raft.response.RaftResponse.*;

public record RaftClientHandler(RaftState raftState,
                                SocketServer raftServer,
                                SocketClient socketClient) implements SocketClientHandler {
    private final static Logger logger = LogManager.getLogger("RaftClientHandler");

    @Override
    public void handleConnected(ServerNode node) {
    }

    @Override
    public void handleResponse(SocketResponse response) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(response.toBytes());
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

            /* 客户端请求成功，则向客户端响应[CLIENT_REQUEST_SUCCESS] */
            case CLIENT_REQUEST_SUCCESS -> {
                    logger.info("[{}] Client request success.", raftState.currentNode());
//                    raftServer.offerInterruptibly(new SocketResponse((SelectionKey) response.attachment(),
//                            new byte[]{CLIENT_REQUEST_SUCCESS}, null));
            }

            /* 客户端请求失败，则向客户端响应[CLIENT_REQUEST_FAILURE] */
            case CLIENT_REQUEST_FAILURE -> {}
//                    raftServer.offerInterruptibly(new SocketResponse((SelectionKey) response.attachment(),
//                            new byte[]{CLIENT_REQUEST_FAILURE}, null));
        }
    }

    @Override
    public void handleAfterLatchAwait() throws Exception {
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) throws IOException {
        if(term > raftState.currentTerm()) {
            raftState.setCurrentTerm(term);
            logger.info("[{}] set [currentTerm] to {}, raft request failure.",
                    raftState.currentNode(), term);
            return;
        }

        switch (status) {
            case REQUEST_VOTE_SUCCESS -> {
                raftState.setVotedNodeAndCheck(node);
                logger.info("[{}] -- Get Vote from node: {}", raftState.currentNode(), node);
            }
            case APPEND_ENTRIES_SUCCESS -> {
                int matchedIndex = buffer.getInt();
                raftState.nextIndex().put(node, matchedIndex + 1);
                raftState.matchedIndex().put(node, matchedIndex);
                raftState.checkCommitIndex();
            }
            case APPEND_ENTRIES_FAILURE -> {
                int nextIndex = raftState.nextIndex().get(node);
                if(nextIndex == 1) {
                    throw new RuntimeException("nextIndex is 1, [APPEND_ENTRIES] should success.");
                }
                raftState.nextIndex().put(node, nextIndex - 1);
            }
        }
    }
}
