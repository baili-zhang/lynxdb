package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.client.RaftClientHandler;
import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.raft.server.RaftServerHandler;
import zbl.moonlight.raft.state.RaftState;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final RaftServer raftServer;
    private final RaftClient raftClient;

    MoonlightServer() throws IOException {
        Configuration config = Configuration.getInstance();
        logger.info("Configuration: [{}]", config);

        ServerNode current = config.currentNode();

        raftClient = new RaftClient();
        raftServer = new RaftServer(current, raftClient);

        RaftState raftState = RaftState.getInstance();
        raftState.raftClient(raftClient);
        raftState.raftServer(raftServer);

        raftServer.setHandler(new RaftServerHandler(raftServer));
        raftServer.setClientHandler(new RaftClientHandler(raftServer, raftClient));
    }

    public void run() {
        Executor.start(raftServer);
        Executor.start(raftClient);
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
