package zbl.moonlight.core.raft.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.client.RaftClient;
import zbl.moonlight.core.raft.request.AppendEntries;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.raft.response.BytesConvertable;
import zbl.moonlight.core.raft.response.ClientResult;
import zbl.moonlight.core.raft.response.RaftResponse;
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
import java.util.concurrent.ConcurrentHashMap;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    private final RaftState raftState;
    private final SocketServer socketServer;
    private final RaftClient raftClient;

    public RaftServerHandler(SocketServer server, Appliable stateMachine,
                             RaftClient client, RaftState state) throws IOException {
        socketServer = server;
        raftState = state;
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
                    /* 重置选举计时器 */
                    raftState.resetElectionTime();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            case RaftRequest.CLIENT_REQUEST -> {
                try {
                    handleClientRequest(request.selectionKey(), buffer);
                } catch (IOException e) {
                    e.printStackTrace();
                }
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

        logger.debug("Handle [RequestVote] RPC request: { candidate: {}, term: {}, " +
                "lastLogIndex: {}, lastLogTerm: {} }", candidate, term, lastLogIndex, lastLogTerm);

        if(term < currentTerm) {
            byte[] data = RaftResponse.requestVoteFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
            raftState.setVoteFor(null);
        }

        if (voteFor == null || voteFor.equals(candidate)) {
            if(lastLogTerm > lastEntry.term()) {
                byte[] data = RaftResponse.requestVoteSuccess(currentTerm, raftState.currentNode());
                sendResult(selectionKey, data);
                return;
            } else if (lastLogTerm == lastEntry.term()) {
                if(lastLogIndex >= raftState.lastEntryIndex()) {
                    byte[] data = RaftResponse.requestVoteSuccess(currentTerm, raftState.currentNode());
                    sendResult(selectionKey, data);
                    return;
                }
            }
        }

        byte[] data = RaftResponse.requestVoteFailure(currentTerm, raftState.currentNode());
        sendResult(selectionKey, data);
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
        Entry leaderPrevEntry = raftState.getEntryByIndex(prevLogIndex);

        if(term < currentTerm) {
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
            raftState.setLeaderNode(leader);
        }

        if(leaderPrevEntry == null) {
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        if(leaderPrevEntry.term() != prevLogTerm) {
            raftState.setMaxIndex(prevLogIndex);
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        raftState.append(entries);
        byte[] data = RaftResponse.appendEntriesSuccess(currentTerm, raftState.currentNode(),
                raftState.lastEntryIndex());
        sendResult(selectionKey, data);

        if(leaderCommit > raftState.commitIndex()) {
            raftState.setCommitIndex(Math.min(leaderCommit, raftState.lastEntryIndex()));
        }

        if(raftState.commitIndex() > raftState.lastApplied()) {
            raftState.apply(raftState.getEntriesByRange(raftState.lastApplied(),
                    raftState.commitIndex()));
        }
    }

    private void handleClientRequest(SelectionKey selectionKey, ByteBuffer buffer) throws IOException {
        /* 把 buffer 中没读到的字节数全部拿出来 */
        int len = buffer.limit() - buffer.position();
        byte[] command = new byte[len];
        buffer.get(command);

        switch (raftState.raftRole()) {
            /* leader 获取到客户端请求，
            需要将请求重新封装成 AppendEntries 请求发送给 raftClient */
            case Leader -> {
                raftState.append(new Entry(raftState.currentTerm(), command));
                ConcurrentHashMap<ServerNode, Integer> nextIndex = raftState.nextIndex();
                for(ServerNode node : nextIndex.keySet()) {
                    int index = nextIndex.get(node);
                    Entry lastEntry = raftState.getEntryByIndex(index);
                    Entry[] entries = raftState.getEntriesByRange(index, raftState.lastEntryIndex());
                    /* 创建 AppendEntries 请求 */
                    AppendEntries appendEntries = new AppendEntries(raftState.currentNode(),
                            raftState.currentTerm(), index, lastEntry.term(),
                            raftState.commitIndex(), entries);
                    /* 将请求发送到其他节点 */
                    SocketRequest request = SocketRequest
                            .newUnicastRequest(appendEntries.toBytes(), node);
                    raftClient.offer(request);
                }
                /* 发送完 AppendEntries 请求后，需要重置心跳计时器 */
                raftState.resetHeartbeatTime();
            }

            /* follower 获取到客户端请求，需要将请求重定向给 leader */
            case Follower -> {
                if(raftState.leaderNode() == null) {
                    sendResult(selectionKey, null);
                } else {
                    SocketRequest request = SocketRequest
                            .newUnicastRequest(command, raftState.leaderNode());
                    raftClient.offer(request);
                }
            }

            /* candidate 获取到客户端请求，直接拒绝 */
            case Candidate -> {

            }
        }
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
        int len = buffer.limit() - buffer.position();
        byte[] command = new byte[len];
        buffer.get(command);

        return new Entry(term, command);
    }


    private void sendResult(SelectionKey selectionKey, byte[] data) {
        SocketResponse response = new SocketResponse(selectionKey, data, null);
        socketServer.offer(response);
    }
}
