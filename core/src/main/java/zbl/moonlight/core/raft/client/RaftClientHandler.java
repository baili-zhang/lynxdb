package zbl.moonlight.core.raft.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.AppendEntries;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RequestVote;
import zbl.moonlight.core.raft.state.RaftRole;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.request.SocketRequest;
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
    public void handleAfterLatchAwait() throws Exception {
        /* 连接未连接的节点 */
        boolean connect = false;
        for(ServerNode node : raftState.otherNodes()) {
            if(!socketClient.isConnecting(node) && !socketClient.isConnected(node)) {
                socketClient.connect(node);
                connect = true;
            }
        }

        if(connect) {
            socketClient.interrupt();
        }
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) throws IOException {
        if(term > raftState.currentTerm()) {
            raftState.setCurrentTerm(term);
            logger.info("[{}] set [currentTerm] to {}", raftState.currentNode(), term);
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
                raftState.nextIndex().put(node, nextIndex - 1);
            }
        }
    }
}
