package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.raft.log.RaftLog;
import com.bailizhang.lynxdb.raft.log.RaftLogEntry;
import com.bailizhang.lynxdb.raft.request.RaftRequest;
import com.bailizhang.lynxdb.raft.response.RaftResponse;
import com.bailizhang.lynxdb.raft.state.RaftRole;
import com.bailizhang.lynxdb.raft.state.RaftState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.NumberUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    private final RaftState raftState;
    private final RaftServer raftServer;
    private final LogIndexMap logIndexMap;

    public RaftServerHandler(RaftServer server) {
        raftServer = server;
        raftState = RaftState.getInstance();
        logIndexMap = new LogIndexMap();
    }

    @Override
    public void handleRequest(SocketRequest request) throws Exception {
        int serial = request.serial();
        byte[] data = request.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.REQUEST_VOTE ->
                    handleRequestVoteRpc(request.selectionKey(), serial, buffer);
            case RaftRequest.APPEND_ENTRIES ->
                    handleAppendEntriesRpc(request.selectionKey(), serial, buffer);
            case RaftRequest.CLIENT_REQUEST ->
                    handleClientRequest(request.selectionKey(), serial, buffer);
        }
    }

    @Override
    public void handleAfterLatchAwait() {
        final int commitIndex = raftState.commitIndex();
        for(SelectionKey key : logIndexMap.keySet()) {
            Integer logIndex = logIndexMap.peek(key);
            while (logIndex != null && logIndex <= commitIndex) {
                byte[] data = RaftResponse.clientRequestSuccessWithoutResult();
                WritableSocketResponse response = new WritableSocketResponse(key, 0, data);
                raftServer.offerInterruptibly(response);
                logIndex = logIndexMap.peekAfterPoll(key);
            }
        }
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer)
            throws IOException {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode candidate = new ServerNode(host, port);
        int term = buffer.getInt();
        int lastLogIndex = buffer.getInt();
        int lastLogTerm = buffer.getInt();

        int currentTerm = raftState.currentTerm();
        RaftLogEntry lastRaftLogEntry = raftState.lastEntry();

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
            raftState.raftRole(RaftRole.FOLLOWER);
        }

        /* 获取 voteFor 的节点，需要在 setCurrentTerm 操作之后 */
        ServerNode voteFor = raftState.voteFor();
        logger.debug("[{}] -- Current [voteFor] is {}",
                raftState.currentNode(), voteFor);

        if (voteFor == null || voteFor.equals(candidate)) {
            if(lastLogTerm > lastRaftLogEntry.term()) {
                requestVoteSuccess(currentTerm, candidate, selectionKey);
                return;
            } else if (lastLogTerm == lastRaftLogEntry.term()) {
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

    private void handleAppendEntriesRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) throws IOException {
        String host = getString(buffer);
        int port = buffer.getInt();
        ServerNode leader = new ServerNode(host, port);
        int term = buffer.getInt();
        int prevLogIndex = buffer.getInt();
        int prevLogTerm = buffer.getInt();
        int leaderCommit = buffer.getInt();
        RaftLogEntry[] entries = BufferUtils.isOver(buffer)
                ? new RaftLogEntry[0] : getEntries(buffer);

        ServerNode currentNode = raftState.currentNode();
        int currentTerm = raftState.currentTerm();
        RaftLogEntry leaderPrevRaftLogEntry = raftState.getEntryByIndex(prevLogIndex);

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
            raftState.raftRole(RaftRole.FOLLOWER);
        }

        /* 只有在 leader 的 term >= currentTerm 时，才重设选举计时器 */
        raftState.resetElectionTimeout();
        /* 设置 leaderNode, 收到客户端请求，将请求重定向给 leader 时用 */
        raftState.leaderNode(leader);
        logger.debug("[{}] Received [AppendEntries], reset election timeout.",
                currentNode);

        /* raft 日志不匹配，AppendEntries 请求失败 */
        if(leaderPrevRaftLogEntry == null) {
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        /* raft 日志不匹配，AppendEntries 请求失败 */
        if(leaderPrevRaftLogEntry != RaftLog.BEGIN_RAFT_LOG_ENTRY && leaderPrevRaftLogEntry.term() != prevLogTerm) {
            raftState.setMaxIndex(prevLogIndex);
            byte[] data = RaftResponse.appendEntriesFailure(currentTerm, raftState.currentNode());
            sendResult(selectionKey, data);
            return;
        }

        raftState.setMaxIndex(prevLogIndex);
        raftState.logAppend(entries);
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

    private void handleClientRequest(SelectionKey selectionKey, int serial, ByteBuffer buffer) throws IOException {
        // 把 buffer 中没读到的字节数全部拿出来
        byte[] command = BufferUtils.getRemaining(buffer);

        logger.info("Handle client request, data: {}", new String(command));

        ServerNode currentNode = raftState.currentNode();

        switch (raftState.raftRole()) {

            // Leader 状态：
            //  请求 -> 写文件 -> 双向队列 -> 发 AppendEntries 请求给 follower
            case LEADER -> {
                // 封装请求
                RaftLogEntry entry = new RaftLogEntry(
                        selectionKey,
                        serial,
                        raftState.currentTerm(),
                        RaftState.DATA_CHANGE,
                        command
                );

                // 尾部追加日志条目
                raftState.appendEntry(entry);
                // 重置心跳计时器
                raftState.resetHeartbeatTimeout();
                // 发送 AppendEntries 请求
                raftState.sendAppendEntries();
            }

            // Follower 状态：
            //  将请求重定向给 leader
            case FOLLOWER -> {
                ServerNode leaderNode = raftState.leaderNode();

                if(leaderNode == null) {
                    logger.info("[{}] is [Follower], leader is [null].", currentNode);

                    byte[] data = ByteBuffer.allocate(1).put(RaftResponse.LEADER_NOT_FOUND).array();
                    raftServer.offerInterruptibly(new WritableSocketResponse(selectionKey, serial, data));

                    return;
                }

                byte[] leader = leaderNode.toString().getBytes(StandardCharsets.UTF_8);
                byte[] data = ByteBuffer.allocate(leader.length + 1)
                        .put(RaftResponse.CLIENT_REQUEST_REDIRECT)
                        .put(leader).array();

                raftServer.offerInterruptibly(new WritableSocketResponse(selectionKey, serial, data));
            }

            // Candidate 状态：
            //  不能重定向请求
            //  不能处理请求
            case CANDIDATE -> {
                logger.info("[{}] is [Candidate], client request failure.", currentNode);

                byte[] data = ByteBuffer.allocate(1).put(RaftResponse.LEADER_NOT_FOUND).array();
                raftServer.offerInterruptibly(new WritableSocketResponse(selectionKey, serial, data));
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

    private RaftLogEntry[] getEntries(ByteBuffer buffer) {
        List<RaftLogEntry> entries = new ArrayList<>();
        while (!BufferUtils.isOver(buffer)) {
            entries.add(getEntry(buffer));
        }

        return entries.toArray(RaftLogEntry[]::new);
    }

    private RaftLogEntry getEntry(ByteBuffer buffer) {
        int len = buffer.getInt() - NumberUtils.INT_LENGTH;
        int term = buffer.getInt();
        byte[] command = new byte[len];
        buffer.get(command);

        return new RaftLogEntry(null, 0, term, RaftState.DATA_CHANGE, command);
    }

    private void sendResult(SelectionKey selectionKey, byte[] data) {
        WritableSocketResponse response = new WritableSocketResponse(selectionKey, 0, data);
        raftServer.offer(response);
    }
}
