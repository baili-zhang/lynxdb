package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.raft.log.LogEntry;
import com.bailizhang.lynxdb.raft.request.AppendEntriesArgs;
import com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs;
import com.bailizhang.lynxdb.raft.request.RaftRequest;
import com.bailizhang.lynxdb.raft.request.RequestVoteArgs;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.LeaderNotExistedResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.result.RedirectResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;

import static com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs.IS_DONE;
import static com.bailizhang.lynxdb.raft.request.InstallSnapshotArgs.NOT_DONE;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    private final RaftState raftState;
    private final RaftServer raftServer;

    public RaftServerHandler(RaftServer server) {
        raftServer = server;
        raftState = RaftState.getInstance();
    }

    @Override
    public void handleRequest(SocketRequest request) {
        int serial = request.serial();
        byte[] data = request.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte method = buffer.get();

        switch (method) {
            case RaftRequest.REQUEST_VOTE ->
                    handleRequestVoteRpc(request.selectionKey(), serial, buffer);
            case RaftRequest.APPEND_ENTRIES ->
                    handleAppendEntriesRpc(request.selectionKey(), serial, buffer);
            case RaftRequest.INSTALL_SNAPSHOT ->
                    handleInstallSnapshot(request.selectionKey(), serial, buffer);
            case RaftRequest.CLIENT_REQUEST ->
                    handleClientRequest(request.selectionKey(), serial, buffer);
        }
    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        RequestVoteArgs args = RequestVoteArgs.from(buffer);

        int term = args.term();
        ServerNode candidate  = args.candidate();
        int lastLogIndex = args.lastLogIndex();
        int lastLogTerm = args.lastLogTerm();

        RequestVoteResult result = raftState.handleRequestVote(term, candidate, lastLogIndex, lastLogTerm);

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

        AppendEntriesResult result = raftState.handleAppendEntries(
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

        InstallSnapshotResult result = switch (done) {
            case NOT_DONE -> raftState.handleInstallSnapshotDone();
            case IS_DONE -> raftState.handleInstallSnapshotNotDone();

            default -> throw new UnsupportedOperationException();
        };

        WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
        raftServer.offerInterruptibly(response);
    }

    private void handleClientRequest(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        if(raftState.isLeader()) {
            byte[] command = BufferUtils.getRemaining(buffer);
            raftState.handleClientRequest(command);
        } else if(raftState.hasLeader()) {
            RedirectResult result = new RedirectResult(raftState.leaderNode());
            WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
            raftServer.offerInterruptibly(response);
        } else {
            LeaderNotExistedResult result = new LeaderNotExistedResult();
            WritableSocketResponse response = new WritableSocketResponse(selectionKey, serial, result);
            raftServer.offerInterruptibly(response);
        }
    }
}
