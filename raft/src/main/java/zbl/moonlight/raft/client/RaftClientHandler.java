package zbl.moonlight.raft.client;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.response.SocketResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.raft.response.RaftResponse.*;

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
        ByteBuffer buffer = ByteBuffer.wrap(null);
        byte status = buffer.get();

        switch (status) {
            case REQUEST_VOTE_SUCCESS, REQUEST_VOTE_FAILURE,
                    APPEND_ENTRIES_SUCCESS, APPEND_ENTRIES_FAILURE -> {
                int term = buffer.getInt();
                int len = buffer.getInt();
                byte[] host = new byte[len];
                buffer.get(host);
                int port = buffer.getInt();
                ServerNode node = new ServerNode(new String(host), port);
                handleRaftRpcResponse(status, term, node, buffer);
            }

            /* 客户端请求成功，则向客户端响应[CLIENT_REQUEST_SUCCESS] */
            case CLIENT_REQUEST_SUCCESS -> {
//                    logger.info("[{}] Client request success.", raftState.currentNode());
//                    raftServer.offerInterruptibly(new SocketResponse((SelectionKey) response.attachment(),
//                            new byte[]{CLIENT_REQUEST_SUCCESS}, null));
            }

            /* 客户端请求失败，则向客户端响应[CLIENT_REQUEST_FAILURE] */
            case CLIENT_REQUEST_FAILURE -> {}
//                    raftServer.offerInterruptibly(new SocketResponse((SelectionKey) response.attachment(),
//                            new byte[]{CLIENT_REQUEST_FAILURE}, null));
        }
    }

    @Override
    public void handleAfterLatchAwait() throws Exception {
    }

    private void handleRaftRpcResponse(byte status, int term, ServerNode node, ByteBuffer buffer) throws IOException {
        if(term > raftState.currentTerm()) {
//            raftState.setCurrentTerm(term);
//            logger.info("[{}] set [currentTerm] to {}, raft request failure.",
//                    raftState.currentNode(), term);
            return;
        }

        switch (status) {
            case REQUEST_VOTE_SUCCESS -> {
//                raftState.setVotedNodeAndCheck(node);
//                logger.info("[{}] -- Get Vote from node: {}", raftState.currentNode(), node);
            }
            case APPEND_ENTRIES_SUCCESS -> {
//                int matchedIndex = buffer.getInt();
//                raftState.nextIndex().put(node, matchedIndex + 1);
//                raftState.matchedIndex().put(node, matchedIndex);
//                raftState.checkCommitIndex();
            }
            case APPEND_ENTRIES_FAILURE -> {
//                int nextIndex = raftState.nextIndex().get(node);
//                if(nextIndex == 1) {
//                    throw new RuntimeException("nextIndex is 1, [APPEND_ENTRIES] should success.");
//                }
//                raftState.nextIndex().put(node, nextIndex - 1);
            }
        }
    }
}
