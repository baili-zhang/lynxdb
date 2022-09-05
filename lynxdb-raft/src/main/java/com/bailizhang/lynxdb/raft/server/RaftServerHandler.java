package com.bailizhang.lynxdb.raft.server;

import com.bailizhang.lynxdb.raft.request.RaftRequest;
import com.bailizhang.lynxdb.raft.state.RaftState;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class RaftServerHandler implements SocketServerHandler {
    private final static Logger logger = LogManager.getLogger("RaftServerHandler");

    private final RaftState raftState;
    private final RaftServer raftServer;

    public RaftServerHandler(RaftServer server) {
        raftServer = server;
        raftState = RaftState.getInstance();
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

    }

    private void handleRequestVoteRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer)
            throws IOException {

    }

    private synchronized void requestVoteSuccess(int currentTerm, ServerNode candidate,
                                    SelectionKey selectionKey) throws IOException {

    }

    private void handleAppendEntriesRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) throws IOException {

    }

    private void handleClientRequest(SelectionKey selectionKey, int serial, ByteBuffer buffer) throws IOException {
        // 把 buffer 中没读到的字节数全部拿出来
        byte[] command = BufferUtils.getRemaining(buffer);

        logger.info("Handle client request, data: {}", new String(command));

        ServerNode currentNode = raftState.currentNode();

        switch (raftState.raftRole()) {
            case LEADER -> {

            }

            case FOLLOWER -> {
            }

            case CANDIDATE -> {
            }
        }
    }
}
