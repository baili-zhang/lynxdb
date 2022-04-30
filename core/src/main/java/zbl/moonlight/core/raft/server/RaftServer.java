package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.client.RaftClient;
import zbl.moonlight.core.raft.state.Appliable;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer {
    private final SocketServer socketServer;

    RaftServer(Appliable stateMachine, ServerNode currentNode) throws IOException {
        socketServer = new SocketServer(new SocketServerConfig(currentNode.port()));
        RaftClient raftClient = new RaftClient();
        socketServer.setHandler(new RaftServerHandler(socketServer, stateMachine,
                raftClient, currentNode));
    }

    public void start() {
        Executor.start(socketServer);
    }
}
