package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.raft.response.AppendEntriesResult;
import com.bailizhang.lynxdb.raft.response.RequestVoteResult;
import com.bailizhang.lynxdb.raft.state.RaftState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.response.SocketResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.*;
import static com.bailizhang.lynxdb.raft.response.AppendEntriesResult.IS_FAILED;
import static com.bailizhang.lynxdb.raft.response.AppendEntriesResult.IS_SUCCESS;
import static com.bailizhang.lynxdb.raft.response.RequestVoteResult.IS_VOTE_GRANTED;
import static com.bailizhang.lynxdb.raft.response.RequestVoteResult.NOT_VOTE_GRANTED;

public class RaftClientHandler implements SocketClientHandler {
    private final static Logger logger = LogManager.getLogger("RaftClientHandler");

    private final RaftServer raftServer;
    private final RaftClient raftClient;
    private final RaftState raftState;

    public RaftClientHandler(RaftServer raftServer, RaftClient raftClient) {
        this.raftServer = raftServer;
        this.raftClient = raftClient;
        raftState = RaftState.getInstance();
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) throws IOException {
        logger.info("Has connected to {}.", ((SocketChannel)selectionKey.channel()).getRemoteAddress());
    }

    @Override
    public void handleResponse(SocketResponse response) {
        SelectionKey selectionKey = response.selectionKey();

        byte[] data = response.data();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte code = buffer.get();

        switch (code) {
            case REQUEST_VOTE -> {
                RequestVoteResult result = RequestVoteResult.from(buffer);
                checkTerm(result.term());

                switch (result.voteGranted()) {
                    case IS_VOTE_GRANTED -> handleVoteGranted(selectionKey);
                    case NOT_VOTE_GRANTED -> handleVoteNotGranted(selectionKey);

                    default -> throw new UnsupportedOperationException();
                }
            }

            case APPEND_ENTRIES -> {
                AppendEntriesResult result = AppendEntriesResult.from(buffer);
                checkTerm(result.term());

                switch (result.success()) {
                    case IS_SUCCESS -> handleAppendEntriesSuccess(selectionKey);
                    case IS_FAILED -> handleAppendEntriesFailed(selectionKey);

                    default -> throw new UnsupportedOperationException();
                }
            }

            case INSTALL_SNAPSHOT -> {

            }

            default -> throw new UnsupportedOperationException();
        }
    }

    private void checkTerm(int term) {

    }

    private void handleVoteGranted(SelectionKey selectionKey) {

    }

    private void handleVoteNotGranted(SelectionKey selectionKey) {

    }

    private void handleAppendEntriesSuccess(SelectionKey selectionKey) {

    }

    private void handleAppendEntriesFailed(SelectionKey selectionKey) {

    }
}
