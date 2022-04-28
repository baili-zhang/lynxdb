package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.raft.client.RaftClient;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.raft.response.RaftResult;
import zbl.moonlight.core.raft.state.Appliable;
import zbl.moonlight.core.raft.state.RaftRole;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketServerHandler;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RaftServerHandler implements SocketServerHandler {
    private final RaftState raftState;
    private final SocketServer socketServer;
    private final RaftClient raftClient;

    public RaftServerHandler(SocketServer server, Appliable stateMachine,
                             RaftClient client) throws IOException {
        socketServer = server;
        raftState = new RaftState(stateMachine);
        raftClient = client;
    }

    @Override
    public void handleRequest(SocketRequest request) {
        byte[] data = request.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.REQUEST_VOTE -> {
                try {
                    handleRequestVoteRpc(request.selectionKey(), buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case RaftRequest.APPEND_ENTRIES -> {
                try {
                    handleAppendEntriesRpc(request.selectionKey(), buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case RaftRequest.CLIENT_REQUEST -> {
                handleClientRequest(request.selectionKey(), buffer);
            }
        }
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, ByteBuffer buffer) throws IOException {
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
    }

    private void handleAppendEntriesRpc(SelectionKey selectionKey, ByteBuffer buffer) throws IOException {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode leader = new ServerNode(host, port);
        int term = buffer.getInt();
        int prevLogIndex = buffer.getInt();
        int prevLogTerm = buffer.getInt();
        int leaderCommit = buffer.getInt();
        Entry[] entries = getEntries(buffer);

        int currentTerm = raftState.currentTerm();
        Entry leaderPrevEntry = raftState.getEntryByCommitIndex(prevLogIndex);

        if(term < currentTerm) {
            sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.FAILURE));
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
        }

        if(leaderPrevEntry == null) {
            sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.FAILURE));
            return;
        }

        if(leaderPrevEntry.term() != prevLogTerm) {
            raftState.resetLogCursor(prevLogIndex);
            sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.FAILURE));
            return;
        }

        raftState.append(entries);
        sendResult(selectionKey, new RaftResult(currentTerm, RaftResult.SUCCESS));

        if(leaderCommit > raftState.commitIndex()) {
            Entry lastEntry = raftState.lastEntry();
            raftState.setCommitIndex(Math.min(leaderCommit, lastEntry.commitIndex()));
        }

        if(raftState.commitIndex() > raftState.lastApplied()) {
            raftState.apply(raftState.getEntriesByRange(raftState.lastApplied(),
                    raftState.commitIndex()));
        }
    }

    private void handleClientRequest(SelectionKey selectionKey, ByteBuffer buffer) {
        switch (raftState.raftRole()) {
            case Leader -> {

            }

            case Follower -> {

            }

            case Candidate -> {

            }
        }

        byte status = SocketState.STAY_CONNECTED_FLAG | SocketState.BROADCAST_FLAG;
        SocketRequest request = SocketRequest.newBroadcastRequest(status, null);
        raftClient.offer(request);
    }

    private byte[] getBytes(ByteBuffer buffer) {
        int len = buffer.getInt();
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return bytes;
    }

    private String getString(ByteBuffer buffer) {
        return new String(getBytes(buffer));
    }

    private Entry[] getEntries(ByteBuffer buffer) {
        int size = buffer.getInt();
        Entry[] entries = new Entry[size];

        for (int i = 0; i < size; i++) {
            entries[i] = getEntry(buffer);
        }

        return entries;
    }

    private Entry getEntry(ByteBuffer buffer) {
        int term = buffer.getInt();
        int commitIndex = buffer.getInt();
        byte method = buffer.get();
        byte[] key = getBytes(buffer);
        byte[] value = getBytes(buffer);

        return new Entry(term, commitIndex, method, key, value);
    }


    private void sendResult(SelectionKey selectionKey, RaftResult result) {
        SocketResponse response = new SocketResponse(selectionKey, result.toBytes());
        socketServer.offer(response);
    }
}
