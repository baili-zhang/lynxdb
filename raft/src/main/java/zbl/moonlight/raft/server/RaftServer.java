package zbl.moonlight.raft.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.client.RaftClientHandler;
import zbl.moonlight.raft.request.AppendEntries;
import zbl.moonlight.raft.request.Entry;
import zbl.moonlight.raft.request.RequestVote;
import zbl.moonlight.raft.state.RaftRole;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.core.timeout.Timeout;
import zbl.moonlight.core.timeout.TimeoutTask;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;
import zbl.moonlight.socket.server.SocketServer;
import zbl.moonlight.socket.server.SocketServerConfig;

import java.io.IOException;
import java.util.List;

public class RaftServer extends SocketServer {
    private static final Logger logger = LogManager.getLogger("RaftServer");

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;

    private final RaftState raftState;
    private final RaftClient raftClient;

    private final Timeout heartbeat;
    private final Timeout election;

    public RaftServer(StateMachine stateMachine, ServerNode currentNode, RaftClient client,
                      List<ServerNode> nodes, String logFilenamePrefix)
            throws IOException {
        super(new SocketServerConfig(currentNode.port()));

        raftClient = client;

        heartbeat = new Timeout(new HeartbeatTask(), HEARTBEAT_INTERVAL_MILLIS);
        /* 设置随机选举超时时间 */
        int ELECTION_INTERVAL_MILLIS = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;
        election = new Timeout(new ElectionTask(), ELECTION_INTERVAL_MILLIS);

        raftState = new RaftState(stateMachine, currentNode, nodes,
                logFilenamePrefix, heartbeat, election);

        logger.info("[{}] -- [ELECTION_INTERVAL_MILLIS] is {}",
                currentNode, ELECTION_INTERVAL_MILLIS);
    }

    @Override
    final protected void doBeforeExecute() {
        Timeout.start(heartbeat);
        Timeout.start(election);
    }

    private class HeartbeatTask implements TimeoutTask {
        private static final Logger logger = LogManager.getLogger("HeartbeatTask");

        @Override
        public void run() {
            try {
                /* 连接未连接的节点 */
                for(ServerNode node : raftState.otherNodes()) {
                    if(!raftClient.isConnecting(node) && !raftClient.isConnected(node)) {
                        raftClient.connect(node);
                    }
                }
                /* 如果心跳超时，则需要发送心跳包 */
                if (raftState.raftRole() == RaftRole.Leader) {
                    for (ServerNode node : raftState.otherNodes()) {
                        int prevLogIndex = raftState.nextIndex().get(node) - 1;
                        int leaderCommit = raftState.commitIndex();

                        int prevLogTerm = prevLogIndex == 0 ? 0
                                : raftState.getEntryTermByIndex(prevLogIndex);
                        Entry[] entries = raftState.getEntriesByRange(prevLogIndex,
                                raftState.indexOfLastLogEntry());

                        AppendEntries appendEntries = new AppendEntries(
                                raftState.currentNode(), raftState.currentTerm(),
                                prevLogIndex, prevLogTerm, leaderCommit,entries);

                        if(raftClient.isConnected(node)) {
                            raftClient.offerInterruptibly(SocketRequest.newUnicastRequest(
                                    appendEntries.toBytes(), node));

                            logger.debug("[{}] send {} to node: {}.", raftState.currentNode(),
                                    appendEntries, node);
                        }
                    }
                }

                /* 心跳超时会中断 socketClient，用来 connect 其他节点 */
                raftClient.interrupt();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ElectionTask implements TimeoutTask {
        private static final Logger logger = LogManager.getLogger("ElectionTask");

        @Override
        public void run() {
            try {
                /* 如果选举超时，需要转换为 Candidate，则向其他节点发送 RequestVote 请求 */
                if (raftState.raftRole() != RaftRole.Leader) {
                    int count = raftState.clusterNodeCount();
                    /* 如果自身节点加上连接上的节点小于或等于半数，则不转换为 Candidate */
                    if(raftClient.connectedNodes().size() + 1 <= (count >> 1)) {
                        return;
                    }

                    /* 转换为 Candidate 角色 */
                    raftState.transformToCandidate();
                    logger.info("[{}] -- [{}] -- Election timeout, " +
                                    "Send RequestVote to other nodes.",
                            raftState.currentNode(), raftState.raftRole());

                    Entry lastEntry = raftState.lastEntry();

                    int term = 0;
                    if (lastEntry != null) {
                        term = lastEntry.term();
                    }

                    byte[] data = new RequestVote(raftState.currentNode(),
                            raftState.currentTerm(),
                            raftState.indexOfLastLogEntry(),
                            term).toBytes();

                    raftClient.offerInterruptibly(SocketRequest.newBroadcastRequest(data));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
