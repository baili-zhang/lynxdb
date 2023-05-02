package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.core.utils.SocketUtils;
import com.bailizhang.lynxdb.raft.core.RaftRpcResultHandler;
import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.PreVoteResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;
import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.*;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.LDTP;
import static com.bailizhang.lynxdb.ldtp.result.ResultType.REDIRECT;

public class RaftClientHandler implements SocketClientHandler {
    private static final Logger logger = LoggerFactory.getLogger(RaftClientHandler.class);

    private final RaftRpcResultHandler raftRpcResultHandler = new RaftRpcResultHandler();

    public RaftClientHandler() {
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) {
        logger.info("Connect to raft node: {}", SocketUtils.address(selectionKey));
    }

    @Override
    public void handleResponse(SocketResponse response) {
        SelectionKey selectionKey = response.selectionKey();

        byte[] data = response.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte type = buffer.get();

        switch (type) {
            case LDTP -> throw new RuntimeException();
            case RAFT_RPC -> handleRaftRpcResult(selectionKey, buffer);
            case REDIRECT -> handleRedirect(selectionKey, buffer);
        }
    }

    @Override
    public void handleDisconnect(SelectionKey selectionKey) {
        logger.info("Disconnect from raft node: {}", SocketUtils.address(selectionKey));
    }

    private void handleRaftRpcResult(SelectionKey selectionKey, ByteBuffer buffer) {
        byte resultMethod = buffer.get();

        switch (resultMethod) {
            case PRE_VOTE_RESULT -> {
                PreVoteResult result = PreVoteResult.from(buffer);
                int term = result.term();
                byte voteGranted = result.voteGranted();

                raftRpcResultHandler.handlePreVoteResult(selectionKey, term, voteGranted);
            }

            case REQUEST_VOTE_RESULT -> {
                RequestVoteResult result = RequestVoteResult.from(buffer);
                int term = result.term();
                byte voteGranted = result.voteGranted();

                raftRpcResultHandler.handleRequestVoteResult(selectionKey, term, voteGranted);
            }

            case APPEND_ENTRIES_RESULT -> {
                AppendEntriesResult result = AppendEntriesResult.from(buffer);
                int term = result.term();
                byte voteGranted = result.success();

                raftRpcResultHandler.handleAppendEntriesResult(selectionKey, term, voteGranted);
            }

            case INSTALL_SNAPSHOT_RESULT -> {
                InstallSnapshotResult result = InstallSnapshotResult.from(buffer);
                int term = result.term();

                raftRpcResultHandler.handleInstallSnapshotResult(selectionKey, term);
            }

            default -> throw new UnsupportedOperationException();
        }
    }

    private void handleRedirect(SelectionKey selectionKey, ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }
}
