package zbl.moonlight.raft.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.log.RaftLog;
import zbl.moonlight.raft.request.AppendEntries;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.request.RaftRequest;
import zbl.moonlight.raft.response.RaftResponse;
import zbl.moonlight.raft.state.RaftRole;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.interfaces.SocketServerHandler;
import zbl.moonlight.socket.request.SocketRequest;
import zbl.moonlight.socket.request.WritableSocketRequest;
import zbl.moonlight.socket.response.WritableSocketResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static zbl.moonlight.raft.response.RaftResponse.CLIENT_REQUEST_FAILURE;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    private final RaftState raftState;
    private final RaftServer raftServer;
    private final RaftClient raftClient;
    private final LogIndexMap logIndexMap;

    public RaftServerHandler(RaftServer server, RaftClient client) {
        raftServer = server;
        raftState = RaftState.getInstance();
        raftClient = client;
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
        final int commitIndex = raftState.commitIndex();
        for(SelectionKey key : logIndexMap.keySet()) {
            Integer logIndex = logIndexMap.peek(key);
            while (logIndex != null && logIndex <= commitIndex) {
                byte[] data = RaftResponse.clientRequestSuccessWithoutResult();
                WritableSocketResponse response = new WritableSocketResponse(key, 0L, data);
                raftServer.offerInterruptibly(response);
                logIndex = logIndexMap.peekAfterPoll(key);
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
            raftState.resetElectionTimeout();
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
        Entry[] entries = EnhanceByteBuffer.isOver(buffer)
                ? new Entry[0] : getEntries(buffer);

        ServerNode currentNode = raftState.currentNode();
        int currentTerm = raftState.currentTerm();
        Entry leaderPrevEntry = raftState.getEntryByIndex(prevLogIndex);

        if(entries.length != 0) {
            logger.info("[{}] -- [AppendEntries] -- leader={}, term={}, " +
                            "prevLogIndex={}, prevLogTerm={}, leaderCommit={} -- {}",
                    currentNode, leader, term, prevLogIndex, prevLogTerm, leaderCommit,
                    Arrays.toString(entries));
        }


        /* Term 不匹配，AppendEntries 请求失败 */
        if(term < currentTerm) {
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        } else if(term > currentTerm) {
            raftState.setCurrentTerm(term);
            raftState.setRaftRole(RaftRole.Follower);
        }

        /* 只有在 leader 的 term >= currentTerm 时，才重设选举计时器 */
        raftState.resetElectionTimeout();
        /* 设置 leaderNode, 收到客户端请求，将请求重定向给 leader 时用 */
        raftState.setLeaderNode(leader);
        logger.debug("[{}] Received [AppendEntries], reset election timeout.",
                currentNode);

        /* raft 日志不匹配，AppendEntries 请求失败 */
        if(leaderPrevEntry == null) {
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        /* raft 日志不匹配，AppendEntries 请求失败 */
        if(leaderPrevEntry != RaftLog.BEGIN_ENTRY && leaderPrevEntry.term() != prevLogTerm) {
            raftState.setMaxIndex(prevLogIndex);
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        raftState.setMaxIndex(prevLogIndex);
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

        ServerNode currentNode = raftState.currentNode();

        if(flag == ClientRequest.RAFT_CLIENT_REQUEST_GET) {
            logger.info("[{}] do client [RAFT_CLIENT_REQUEST_GET] request.", currentNode);
            return;
        }

        logger.info("[{}] do client [RAFT_CLIENT_REQUEST_SET] request, command is: {}",
                currentNode, new String(command));

        /* 如果是 RAFT_CLIENT_REQUEST_SET，则执行以下逻辑 */
        switch (raftState.raftRole()) {
            /* leader 获取到客户端请求，需要将请求重新封装成 AppendEntries 请求发送给 socketClient */
            case Leader -> {
                int logIndex = raftState.append(new Entry(raftState.currentTerm(), command));
                logger.info("[{}] -- [Leader] -- Received client request, log max index is {}.",
                        currentNode, logIndex);

                raftState.resetHeartbeatTimeout();
                ConcurrentHashMap<SelectionKey, Integer> nextIndex = raftState.nextIndex();
                for(SelectionKey key : nextIndex.keySet()) {
                    int prevLogIndex = logIndex - 1;
                    int prevLogTerm = prevLogIndex == 0 ? 0
                            : raftState.getEntryTermByIndex(prevLogIndex);
                    Entry[] entries = raftState.getEntriesByRange(prevLogIndex,
                            raftState.indexOfLastLogEntry());
                    /* 创建 AppendEntries 请求 */
                    AppendEntries appendEntries = new AppendEntries();
                    /* 将请求发送到其他节点 */
                    WritableSocketRequest request = new WritableSocketRequest(key, (byte) 0x00,
                            0L, appendEntries.toBytes());
                    raftClient.offerInterruptibly(request);

                    logger.info("[{}] -- Send [{}] to {}", currentNode, appendEntries,
                            ((SocketChannel)key.channel()).getRemoteAddress());
                }

                /* 重设心跳计时器 */
                raftState.resetHeartbeatTimeout();

                /* 将 logIndex 缓存到 logIndexMap */
                logIndexMap.offer(selectionKey, logIndex);
            }

            case Follower -> {
                /* follower 获取到客户端请求，需要将请求重定向给 leader */
            }

            /* candidate 获取到客户端请求，直接拒绝 */
            case Candidate -> {
                logger.info("[{}] is [Candidate], client request failure.", currentNode);
                byte[] data = ByteBuffer.allocate(1).put(CLIENT_REQUEST_FAILURE).array();
                raftServer.offerInterruptibly(new WritableSocketResponse(selectionKey, 0L, data));
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
        List<Entry> entries = new ArrayList<>();
        while (!EnhanceByteBuffer.isOver(buffer)) {
            entries.add(getEntry(buffer));
        }

        return entries.toArray(Entry[]::new);
    }

    private Entry getEntry(ByteBuffer buffer) {
        int len = buffer.getInt() - NumberUtils.INT_LENGTH;
        int term = buffer.getInt();
        byte[] command = new byte[len];
        buffer.get(command);

        return new Entry(term, command);
    }

    private void sendResult(SelectionKey selectionKey, byte[] data) {
        WritableSocketResponse response = new WritableSocketResponse(selectionKey, 0L, data);
        raftServer.offer(response);
    }
}
