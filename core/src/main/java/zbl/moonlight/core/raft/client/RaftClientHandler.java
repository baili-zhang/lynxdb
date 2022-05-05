package zbl.moonlight.core.raft.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RequestVote;
import zbl.moonlight.core.raft.state.RaftRole;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
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
                                RaftClient raftClient) implements SocketClientHandler {
    private final static Logger logger = LogManager.getLogger("RaftClientHandler");

    @Override
    public void handleConnected(ServerNode node) {
    }

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
        if (raftState.raftRole() == RaftRole.Leader && raftState.isHeartbeatTimeout()) {
            logger.info("Heartbeat timeout, need to send AppendEntries to other nodes.");
            /* 重置心跳计时器 */
            raftState.resetHeartbeatTime();
        }
        /* 如果选举超时，需要转换为 Candidate，则向其他节点发送 RequestVote 请求 */
        if (raftState.isElectionTimeout()) {
            raftState.setRaftRole(RaftRole.Candidate);
            try {
                raftState.setCurrentTerm(raftState.currentTerm() + 1);
                raftState.setVoteFor(raftState.currentNode());
            } catch (IOException e) {
                e.printStackTrace();
            }

            logger.info("[{}]Election timeout, Send RequestVote to other nodes.",
                    raftState.raftRole());

            Entry lastEntry = null;
            try {
                lastEntry = raftState.lastEntry();
            } catch (IOException e) {
                e.printStackTrace();
            }

            int term = 0;
            if (lastEntry != null) {
                term = lastEntry.term();
            }

            byte[] data = new byte[0];
            try {
                data = new RequestVote(raftState.currentNode(),
                        raftState.currentTerm(),
                        raftState.lastEntryIndex(),
                        term).toBytes();
            } catch (IOException e) {
                e.printStackTrace();
            }
            raftClient.offer(SocketRequest.newBroadcastRequest(data));
            /* 重置选举计时器 */
            raftState.resetElectionTime();
        }
        /* 连接未连接的节点 */
        for(ServerNode node : raftState.otherNodes()) {
            if(!raftClient.isConnecting(node) && !raftClient.isConnected(node)) {
                try {
                    raftClient.connect(node);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) {
        switch (status) {
            case REQUEST_VOTE_SUCCESS -> {
                raftState.setVotedNodeAndCheck(node);
                logger.info("Get Vote from node: {}", node);
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
