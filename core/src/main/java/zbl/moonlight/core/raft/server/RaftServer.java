package zbl.moonlight.core.raft.server;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.result.RaftResult;
import zbl.moonlight.core.raft.state.Appliable;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;

public class RaftServer {
    private static final int DEFAULT_RAFT_PORT = 7920;

    private final SocketServer socketServer;

    RaftServer(Appliable stateMachine) throws IOException {
        socketServer = new SocketServer(new SocketServerConfig(DEFAULT_RAFT_PORT));
        socketServer.setHandler(new RaftServerHandler(socketServer, stateMachine));
    }

    public void start() {
        Executor.start(socketServer);
    }
}
