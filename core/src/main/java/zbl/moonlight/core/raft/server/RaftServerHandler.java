package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.raft.result.RaftResult;
import zbl.moonlight.core.raft.state.Appliable;
import zbl.moonlight.core.raft.state.RaftRole;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketServerHandler;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RaftServerHandler implements SocketServerHandler {
    private final RaftState raftState;
    private final SocketServer socketServer;

    public RaftServerHandler(SocketServer server, Appliable stateMachine) throws IOException {
        socketServer = server;
        raftState = new RaftState(stateMachine);
    }

    @Override
    public void handleRequest(SocketRequest request) {
        byte[] data = request.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.REQUEST_VOTE -> handleRequestVoteRpc(request.selectionKey(), buffer);
            case RaftRequest.APPEND_ENTRIES -> handleAppendEntriesRpc(request.selectionKey(), buffer);
        }
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, ByteBuffer buffer) {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode candidate = new ServerNode(host, port);
        int term = buffer.getInt();
        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        int currentTerm = raftState.currentTerm();
        Entry lastEntry = raftState.lastEntry();
        ServerNode voteFor = raftState.voteFor();

        if(term < currentTerm) {
            sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.FAILURE));
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
        }

        if (voteFor == null || voteFor.equals(candidate)) {
            if(lastLogTerm > lastEntry.term()) {
                sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.SUCCESS));
                return;
            } else if (lastLogTerm == lastEntry.term()) {
                if(lastLogIndex >= lastEntry.commitIndex()) {
                    sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.SUCCESS));
                    return;
                }
            }
        }

        sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.FAILURE));
        return;
    }

    private void handleAppendEntriesRpc(SelectionKey selectionKey, ByteBuffer buffer) {
    }

    private String getString(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes);
    }

    private void sendResult(SelectionKey selectionKey, RaftResult result) {
        SocketResponse response = new SocketResponse(selectionKey, result.toBytes());
        socketServer.offer(response);
    }
}
