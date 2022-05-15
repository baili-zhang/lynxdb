package zbl.moonlight.core.raft.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.AppendEntries;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.raft.response.RaftResponse;
import zbl.moonlight.core.raft.state.StateMachine;
import zbl.moonlight.core.raft.state.RaftRole;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.interfaces.SocketServerHandler;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.utils.ByteBufferUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentHashMap;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    public final static byte RAFT_CLIENT_REQUEST_GET = (byte) 0x01;
    public final static byte RAFT_CLIENT_REQUEST_SET = (byte) 0x02;

    private final RaftState raftState;
    private final SocketServer socketServer;
    private final SocketClient socketClient;
    private final LogIndexMap logIndexMap;
    private final StateMachine stateMachine;

    public RaftServerHandler(SocketServer server, StateMachine machine,
                             SocketClient client, RaftState state) {
        socketServer = server;
        stateMachine = machine;
        raftState = state;
        socketClient = client;
        logIndexMap = new LogIndexMap();
    }

    @Override
    public void handleRequest(SocketRequest request) throws Exception {
        byte[] data = request.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.REQUEST_VOTE ->
                    handleRequestVoteRpc(request.selectionKey(), buffer);
            case RaftRequest.APPEND_ENTRIES ->
                    handleAppendEntriesRpc(request.selectionKey(), buffer);
            case RaftRequest.CLIENT_REQUEST ->
                    handleClientRequest(request.selectionKey(), buffer);
        }
    }

    @Override
    public void handleAfterLatchAwait() {
        int commitIndex = raftState.commitIndex();
        for(SelectionKey key : logIndexMap.keySet()) {
            while (logIndexMap.peek(key) <= commitIndex) {
                byte[] data = RaftResponse.clientRequestSuccessWithoutResult();
                SocketResponse response = new SocketResponse(key, data, null);
                socketServer.offerInterruptibly(response);
                logIndexMap.poll(key);
            }
        }
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, ByteBuffer buffer)
            throws IOException {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode candidate = new ServerNode(host, port);
        int term = buffer.getInt();
        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        int currentTerm = raftState.currentTerm();
        Entry lastEntry = raftState.lastEntry();

        logger.debug("Handle [RequestVote] RPC request: { candidate: {}, term: {}, " +
                "lastLogIndex: {}, lastLogTerm: {} }", candidate, term, lastLogIndex, lastLogTerm);

        if(term < currentTerm) {
            byte[] data = RaftResponse.requestVoteFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);

            logger.info("[RequestVote: term({}) < currentTerm({})] -- [{}] " +
                            "-- Has not voted for candidate: {}.", term, currentTerm,
                    raftState.currentNode(), candidate);
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
        }

        /* 获取 voteFor 的节点，需要在 setCurrentTerm 操作之后 */
        ServerNode voteFor = raftState.voteFor();
        logger.debug("[{}] -- Current [voteFor] is {}",
                raftState.currentNode(), voteFor);

        if (voteFor == null || voteFor.equals(candidate)) {
            if(lastLogTerm > lastEntry.term()) {
                requestVoteSuccess(currentTerm, candidate, selectionKey);
                return;
            } else if (lastLogTerm == lastEntry.term()) {
                if(lastLogIndex >= raftState.indexOfLastLogEntry()) {
                    requestVoteSuccess(currentTerm, candidate, selectionKey);
                    return;
                }
            }
        }

        logger.info("[RequestVote: voteFor != null && voteFor.equals(candidate)] -- [{}] " +
                        "-- Has not voted for candidate: {}.",
                raftState.currentNode(), candidate);
        byte[] data = RaftResponse.requestVoteFailure(currentTerm, raftState.currentNode());
        sendResult(selectionKey, data);
    }

    private synchronized void requestVoteSuccess(int currentTerm, ServerNode candidate,
                                    SelectionKey selectionKey) throws IOException {
        ServerNode voteFor = raftState.voteFor();
        if (voteFor == null || voteFor.equals(candidate)) {
            byte[] data = RaftResponse.requestVoteSuccess(currentTerm, raftState.currentNode());
            raftState.setVoteFor(candidate);
            sendResult(selectionKey, data);
            /* 请求投票成功，当前节点的投票给了其他节点，当前节点不可能被选举成 Leader，所以需要重设选举计时器 */
            raftState.resetElectionTime();
        }
    }

    private void handleAppendEntriesRpc(SelectionKey selectionKey, ByteBuffer buffer) throws IOException {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode leader = new ServerNode(host, port);
        int term = buffer.getInt();
        int prevLogIndex = buffer.getInt();
        int prevLogTerm = buffer.getInt();
        int leaderCommit = buffer.getInt();
        Entry[] entries = ByteBufferUtils.isOver(buffer)
                ? new Entry[0] : getEntries(buffer);

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

        /* 只有在 leader 的 term >= currentTerm 时，才重设选举计时器 */
        raftState.resetElectionTime();
        logger.debug("[{}] Received [AppendEntries], reset election timeout.",
                raftState.currentNode());

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
                raftState.indexOfLastLogEntry());
        sendResult(selectionKey, data);

        if(leaderCommit > raftState.commitIndex()) {
            raftState.setCommitIndex(Math.min(leaderCommit, raftState.indexOfLastLogEntry()));
        }

        if(raftState.commitIndex() > raftState.lastApplied()) {
            raftState.apply(raftState.getEntriesByRange(raftState.lastApplied(),
                    raftState.commitIndex()));
        }
    }

    private void handleClientRequest(SelectionKey selectionKey, ByteBuffer buffer) throws IOException {
        /* 将客户端请求的标志拿出来， */
        byte flag = buffer.get();
        /* 把 buffer 中没读到的字节数全部拿出来 */
        int len = buffer.limit() - buffer.position();
        byte[] command = new byte[len];
        buffer.get(command);

        if(flag == RAFT_CLIENT_REQUEST_GET) {
            stateMachine.exec(selectionKey, command);
            return;
        }

        /* 如果是 RAFT_CLIENT_REQUEST_SET，则执行以下逻辑 */
        switch (raftState.raftRole()) {
            /* leader 获取到客户端请求，需要将请求重新封装成 AppendEntries 请求发送给 socketClient */
            case Leader -> {
                int logIndex = raftState.append(new Entry(raftState.currentTerm(), command));
                ConcurrentHashMap<ServerNode, Integer> nextIndex = raftState.nextIndex();
                for(ServerNode node : nextIndex.keySet()) {
                    int index = nextIndex.get(node);
                    Entry lastEntry = raftState.getEntryByIndex(index);
                    Entry[] entries = raftState.getEntriesByRange(index, raftState.indexOfLastLogEntry());
                    /* 创建 AppendEntries 请求 */
                    AppendEntries appendEntries = new AppendEntries(raftState.currentNode(),
                            raftState.currentTerm(), index, lastEntry.term(),
                            raftState.commitIndex(), entries);
                    /* 将请求发送到其他节点 */
                    SocketRequest request = SocketRequest
                            .newUnicastRequest(appendEntries.toBytes(), node);
                    socketClient.offer(request);
                }

                /* 重设心跳计时器 */
                raftState.resetHeartbeatTime();

                /* 将 logIndex 缓存到 logIndexMap */
                logIndexMap.offer(selectionKey, logIndex);
            }

            /* follower 获取到客户端请求，需要将请求重定向给 leader */
            case Follower -> {
                if(raftState.leaderNode() == null) {
                    sendResult(selectionKey, null);
                } else {
                    SocketRequest request = SocketRequest
                            .newUnicastRequest(command, raftState.leaderNode());
                    socketClient.offer(request);
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
