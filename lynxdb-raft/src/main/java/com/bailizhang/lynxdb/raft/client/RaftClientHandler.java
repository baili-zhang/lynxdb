package com.bailizhang.lynxdb.raft.client;

import com.bailizhang.lynxdb.raft.response.RaftResponse;
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
    public void handleResponse(SocketResponse response) throws Exception {

    }

    @Override
    public void handleAfterLatchAwait() throws Exception {
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) throws IOException {
        switch (status) {
            case RaftResponse.REQUEST_VOTE_SUCCESS -> {
            }
            case RaftResponse.APPEND_ENTRIES_SUCCESS -> {
            }
            case RaftResponse.APPEND_ENTRIES_FAILURE -> {
            }
        }
    }
}
