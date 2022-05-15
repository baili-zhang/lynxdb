package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.client.RaftClientHandler;
import zbl.moonlight.core.raft.state.StateMachine;
import zbl.moonlight.core.raft.state.RaftState;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;
import java.util.List;

public class RaftServer {
    private static final String DEFAULT_NAME = "RAFT_SERVER";

    private final SocketServer socketServer;
    private final SocketClient socketClient;

    public RaftServer(StateMachine stateMachine, ServerNode currentNode,
                      List<ServerNode> nodes, String logFilenamePrefix)
            throws IOException {
        RaftState raftState = new RaftState(stateMachine, currentNode, nodes,
                logFilenamePrefix);
        socketServer = new SocketServer(new SocketServerConfig(currentNode.port()));
        socketClient = new SocketClient();
        socketClient.setHandler(new RaftClientHandler(raftState, socketServer,
                socketClient));
        socketServer.setHandler(new RaftServerHandler(socketServer, stateMachine,
                socketClient, raftState));
    }

    public void start(String name) {
        Executor.start(socketClient, name + "-client");
        Executor.start(socketServer, name);
    }

    public void start() {
        start(DEFAULT_NAME);
    }

    public SocketServer socketServer() {
        return socketServer;
    }

    public SocketClient socketClient() {
        return socketClient;
    }
}
