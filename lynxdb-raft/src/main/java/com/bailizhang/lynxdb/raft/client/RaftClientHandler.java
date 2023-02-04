package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.raft.result.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.result.InstallSnapshotResult;
import com.bailizhang.lynxdb.raft.result.RequestVoteResult;
import com.bailizhang.lynxdb.raft.state.RaftState;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static com.bailizhang.lynxdb.raft.result.AppendEntriesResult.IS_FAILED;
import static com.bailizhang.lynxdb.raft.result.AppendEntriesResult.IS_SUCCESS;
import static com.bailizhang.lynxdb.raft.result.RaftResult.*;
import static com.bailizhang.lynxdb.raft.result.RequestVoteResult.IS_VOTE_GRANTED;
import static com.bailizhang.lynxdb.raft.result.RequestVoteResult.NOT_VOTE_GRANTED;

public class RaftClientHandler implements SocketClientHandler {
    private final RaftState raftState;

    public RaftClientHandler() {
        raftState = RaftState.getInstance();
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) throws IOException {
    }

    @Override
    public void handleResponse(SocketResponse response) {
        SelectionKey selectionKey = response.selectionKey();

        byte[] data = response.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte code = buffer.get();

        switch (code) {
            case REQUEST_VOTE_RESULT -> {
                RequestVoteResult result = RequestVoteResult.from(buffer);
                int term = result.term();

                raftState.checkTerm(term);

                switch (result.voteGranted()) {
                    case IS_VOTE_GRANTED -> raftState.voteGranted(term, selectionKey);
                    case NOT_VOTE_GRANTED -> raftState.voteNotGranted(term, selectionKey);

                    default -> throw new UnsupportedOperationException();
                }
            }

            case APPEND_ENTRIES_RESULT -> {
                AppendEntriesResult result = AppendEntriesResult.from(buffer);
                raftState.checkTerm(result.term());

                switch (result.success()) {
                    case IS_SUCCESS -> raftState.appendEntriesSuccess(selectionKey);
                    case IS_FAILED -> raftState.appendEntriesFailed(selectionKey);

                    default -> throw new UnsupportedOperationException();
                }
            }

            case INSTALL_SNAPSHOT_RESULT -> {
                InstallSnapshotResult result = InstallSnapshotResult.from(buffer);
                raftState.checkTerm(result.term());
            }

            default -> throw new UnsupportedOperationException();
        }
    }
}
