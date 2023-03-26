package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.common.RaftRole;
import com.bailizhang.lynxdb.raft.core.RaftRpcHandler;
import com.bailizhang.lynxdb.raft.core.RaftState;
import com.bailizhang.lynxdb.raft.core.RaftStateHolder;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs;
import com.bailizhang.lynxdb.raft.request.RaftRequest;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.LeaderNotExistedResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.result.RedirectResult;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs.IS_DONE;
import static com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs.NOT_DONE;

public class RaftServerHandler implements SocketServerHandler {
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
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.PRE_VOTE ->
                    handlePreVoteRpc(selectionKey, serial, buffer);
            case RaftRequest.REQUEST_VOTE ->
                    handleRequestVoteRpc(selectionKey, serial, buffer);
            case RaftRequest.APPEND_ENTRIES ->
                    handleAppendEntriesRpc(selectionKey, serial, buffer);
            case RaftRequest.INSTALL_SNAPSHOT ->
                    handleInstallSnapshot(selectionKey, serial, buffer);
            case RaftRequest.CLIENT_REQUEST ->
                    handleClientRequest(selectionKey, serial, buffer);
        }
    }

    private void handlePreVoteRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        RequestVoteArgs args = RequestVoteArgs.from(buffer);

        int term = args.term();
        ServerNode candidate  = args.candidate();
        int lastLogIndex = args.lastLogIndex();
        int lastLogTerm = args.lastLogTerm();

        RequestVoteResult result = raftRpcHandler.handlePreVote(
                term,
                candidate,
                lastLogIndex,
                lastLogTerm
        );

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
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

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }

    private void handleAppendEntriesRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
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

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }

    private void handleInstallSnapshot(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
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

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }

    private void handleClientRequest(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        RaftState raftState = RaftStateHolder.raftState();
        RaftRole role = raftState.role().get();

        if(role == RaftRole.LEADER) {
            byte[] command = BufferUtils.getRemaining(buffer);
            // raftRpcHandler.handleClientRequest(selectionKey, serial, command);
            return;
        }

        ServerNode leader = raftState.leader().get();

        if(leader != null) {
            RedirectResult result = new RedirectResult(leader);
            WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
            raftServer.offerInterruptibly(response);
            return;
        }

        LeaderNotExistedResult result = new LeaderNotExistedResult();
        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }
}
