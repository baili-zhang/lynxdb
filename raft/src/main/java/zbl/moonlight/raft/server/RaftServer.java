package zbl.moonlight.raft.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.client.RaftClientHandler;
import zbl.moonlight.core.timeout.Timeout;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.server.SocketServer;
import zbl.moonlight.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer extends SocketServer {
    private static final Logger logger = LogManager.getLogger("RaftServer");

    private final RaftClient raftClient;

    public RaftServer(ServerNode currentNode, RaftClient client)
            throws IOException {
        super(new SocketServerConfig(currentNode.port()));
        raftClient = client;
    }

    @Override
    final protected void doBeforeExecute() {
        // 启动心跳超时计时器和选举超时计时器
        RaftState.getInstance().startTimeout();
    }

    public void setClientHandler(RaftClientHandler raftClientHandler) {
        raftClient.setHandler(raftClientHandler);
    }
}
