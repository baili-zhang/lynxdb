package zbl.moonlight.core.raft.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.AppendEntries;
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
        /* 如果心跳超时，则需要发送心跳包 */
        if (raftState.raftRole() == RaftRole.Leader && raftState.isHeartbeatTimeout()) {
            for (ServerNode node : raftState.otherNodes()) {
                int prevLogIndex = raftState.nextIndex().get(node) - 1;
                int leaderCommit = raftState.commitIndex();

                int prevLogTerm = prevLogIndex == 0 ? 0
                        : raftState.getEntryTermByIndex(prevLogIndex);
                Entry[] entries = raftState.getEntriesByRange(prevLogIndex,
                        raftState.indexOfLastLogEntry());

                AppendEntries appendEntries = new AppendEntries(
                        raftState.currentNode(), raftState.currentTerm(),
                        prevLogIndex, prevLogTerm, leaderCommit,entries);

                if(raftClient.isConnected(node)) {
                    raftClient.offer(SocketRequest.newUnicastRequest(
                            appendEntries.toBytes(), node));
                }

                logger.info("[{}] send {} to node: {}.", raftState.currentNode(),
                        appendEntries, node);
            }
            /* 重置心跳计时器 */
            raftState.resetHeartbeatTime();
        }
        /* 如果选举超时，需要转换为 Candidate，则向其他节点发送 RequestVote 请求 */
        if (raftState.raftRole() != RaftRole.Leader && raftState.isElectionTimeout()) {
            raftState.setRaftRole(RaftRole.Candidate);
            raftState.setCurrentTerm(raftState.currentTerm() + 1);
            raftState.setVoteFor(raftState.currentNode());


            logger.info("[{}] -- [{}] -- Election timeout, " +
                            "Send RequestVote to other nodes.",
                    raftState.currentNode(), raftState.raftRole());

            Entry lastEntry = raftState.lastEntry();

            int term = 0;
            if (lastEntry != null) {
                term = lastEntry.term();
            }

            byte[] data = new RequestVote(raftState.currentNode(),
                        raftState.currentTerm(),
                        raftState.indexOfLastLogEntry(),
                        term).toBytes();

            raftClient.offer(SocketRequest.newBroadcastRequest(data));
            /* 重置选举计时器 */
            raftState.resetElectionTime();
        }
        /* 连接未连接的节点 */
        for(ServerNode node : raftState.otherNodes()) {
            if(!raftClient.isConnecting(node) && !raftClient.isConnected(node)) {
                raftClient.connect(node);
            }
        }
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) throws IOException {
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
