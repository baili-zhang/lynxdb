package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.raft.core.*;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs;
import com.bailizhang.lynxdb.raft.request.PreVoteArgs;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.result.*;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.result.RedirectResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.*;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.LDTP_METHOD;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;

public class RaftServerHandler implements SocketServerHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftServerHandler.class);

    private final RaftServer raftServer;
    private final RaftRpcHandler raftRpcHandler;

    public RaftServerHandler(RaftServer server, RaftRpcHandler handler) {
        raftServer = server;
        raftRpcHandler = handler;
    }

    @Override
    public void handleRequest(SocketRequest request) {
        SelectionKey selectionKey = request.selectionKey();
        int serial = request.serial();
        byte[] data = request.data();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte type = buffer.get();

        switch (type) {
            case LDTP_METHOD -> handleNeedPersistenceRequest(selectionKey, serial, buffer);
            case RAFT_RPC -> handleRaftRpc(selectionKey, serial, buffer);
            default -> {
                logger.info("Type is: {}", type);

                throw new RuntimeException();
            }
        }
    }

    private void handleRaftRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        byte method = buffer.get();

        switch (method) {
            case PRE_VOTE -> handlePreVoteRpc(selectionKey, serial, buffer);
            case REQUEST_VOTE -> handleRequestVoteRpc(selectionKey, serial, buffer);
            case APPEND_ENTRIES -> handleAppendEntriesRpc(selectionKey, serial, buffer);
            case INSTALL_SNAPSHOT -> handleInstallSnapshot(selectionKey, serial, buffer);
            case JOIN_CLUSTER -> handleNeedPersistenceRequest(selectionKey, serial, buffer);
            default -> throw new RuntimeException();
        }
    }

    private void handlePreVoteRpc(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer buffer
    ) {
        PreVoteArgs args = PreVoteArgs.from(buffer);

        int term = args.term();
        int lastLogIndex = args.lastLogIndex();
        int lastLogTerm = args.lastLogTerm();

        PreVoteResult result = raftRpcHandler.handlePreVote(
                term,
                lastLogIndex,
                lastLogTerm
        );

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result.toBuffers());
        raftServer.offerInterruptibly(response);
    }

    private void handleRequestVoteRpc(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer buffer
    ) {
        RequestVoteArgs args = RequestVoteArgs.from(buffer);

        int term = args.term();
        ServerNode candidate  = args.candidate();
        int lastLogIndex = args.lastLogIndex();
        int lastLogTerm = args.lastLogTerm();

        RequestVoteResult result = raftRpcHandler.handleRequestVote(
                term,
                candidate,
                lastLogIndex,
                lastLogTerm
        );

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result.toBuffers());
        raftServer.offerInterruptibly(response);
    }

    private void handleAppendEntriesRpc(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer buffer
    ) {
        AppendEntriesArgs args = AppendEntriesArgs.from(buffer);

        int term = args.term();
        ServerNode leader = args.leader();
        int prevLogIndex = args.prevLogIndex();
        int prevLogTerm = args.prevLogTerm();
        List<LogEntry> entries = args.entries();
        int leaderCommit = args.leaderCommit();

        AppendEntriesResult result = raftRpcHandler.handleAppendEntries(
                term,
                leader,
                prevLogIndex,
                prevLogTerm,
                entries,
                leaderCommit
        );

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.toBuffers()
        );
        raftServer.offerInterruptibly(response);
    }

    private void handleInstallSnapshot(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer buffer
    ) {
        InstallSnapshotArgs args = InstallSnapshotArgs.from(buffer);

        int term = args.term();
        ServerNode leader = args.leader();
        int lastIncludedIndex = args.lastIncludedIndex();
        int lastIncludedTerm = args.lastIncludedTerm();
        int offset = args.offset();
        byte[] data = args.data();
        byte done = args.done();

        InstallSnapshotResult result = raftRpcHandler.handleInstallSnapshot(
                term,
                leader,
                lastIncludedIndex,
                lastIncludedTerm,
                offset,
                data,
                done
        );

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.toBuffers()
        );
        raftServer.offerInterruptibly(response);
    }

    private void handleNeedPersistenceRequest(
            SelectionKey selectionKey,
            int serial,
            ByteBuffer buffer
    ) {
        RaftState raftState = RaftStateHolder.raftState();
        RaftRole role = raftState.role().get();

        logger.info("Handle client request, current role is {}", role);

        // 如果当前角色为 leader
        if(role == RaftRole.LEADER) {
            byte[] data = buffer.array();
            int idx = raftRpcHandler.persistenceClientRequest(data);

            ClientRequest request = new ClientRequest(
                    selectionKey,
                    idx,
                    serial,
                    data
            );

            UncommittedClientRequests requests = UncommittedClientRequests.requests();
            requests.add(request);

            logger.info("Add client request to UncommittedClientRequests.");

            RaftTimeWheel.timeWheel().heartbeat();
            return;
        }

        handleIfNotLeader(selectionKey, serial);
    }

    private void handleIfNotLeader(SelectionKey selectionKey, int serial) {
        RaftState raftState = RaftStateHolder.raftState();
        ServerNode leader = raftState.leader().get();

        // 如果当前 leader 存在，则将客户端请求重定向到 leader
        if(leader != null) {
            RedirectResult result = new RedirectResult(leader);
            WritableSocketResponse response = new WritableSocketResponse(
                    selectionKey,
                    serial,
                    result.toBuffers()
            );
            raftServer.offerInterruptibly(response);
            return;
        }

        // 如果 leader 不存在，返回 leader 不存在，不处理当前请求
        LeaderNotExistedResult result = new LeaderNotExistedResult();
        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.toBuffers()
        );
        raftServer.offerInterruptibly(response);
    }
}
