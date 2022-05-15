package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.client.RaftClient;
import zbl.moonlight.core.raft.client.RaftClientHandler;
import zbl.moonlight.core.raft.state.StateMachine;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;
import java.util.List;

public class RaftServer {
    private static final String DEFAULT_NAME = "RAFT_SERVER";

    private final SocketServer socketServer;
    private final RaftClient raftClient;

    public RaftServer(StateMachine stateMachine, ServerNode currentNode,
                      List<ServerNode> nodes, String logFilenamePrefix)
            throws IOException {
        RaftState raftState = new RaftState(stateMachine, currentNode, nodes,
                logFilenamePrefix);
        socketServer = new SocketServer(new SocketServerConfig(currentNode.port()));
        raftClient = new RaftClient();
        raftClient.setHandler(new RaftClientHandler(raftState, socketServer,
                raftClient));
        socketServer.setHandler(new RaftServerHandler(socketServer, stateMachine,
                raftClient, raftState));
    }

    public void start(String name) {
        Executor.start(raftClient, name + "-client");
        Executor.start(socketServer, name);
    }

    public void start() {
        start(DEFAULT_NAME);
    }

    public SocketServer socketServer() {
        return socketServer;
    }
}
