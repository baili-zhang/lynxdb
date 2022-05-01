package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.client.RaftClient;
import zbl.moonlight.core.raft.client.RaftClientHandler;
import zbl.moonlight.core.raft.state.Appliable;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer {
    private final SocketServer socketServer;

    RaftServer(Appliable stateMachine, ServerNode currentNode) throws IOException {
        RaftState raftState = new RaftState(stateMachine, currentNode, null);
        socketServer = new SocketServer(new SocketServerConfig(currentNode.port()));
        RaftClient raftClient = new RaftClient();
        raftClient.setHandler(new RaftClientHandler(raftState, socketServer));
        socketServer.setHandler(new RaftServerHandler(socketServer, stateMachine,
                raftClient, raftState));
    }

    public void start() {
        Executor.start(socketServer);
    }
}
