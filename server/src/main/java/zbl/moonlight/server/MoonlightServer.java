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
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final RaftServer raftServer;
    private final RaftClient raftClient;
    private final EngineExecutor engineExecutor;

    MoonlightServer() throws IOException {
        Configuration config = Configuration.getInstance();
        logger.info("Configuration: [{}]", config);

        ServerNode current = config.currentNode();

        raftClient = new RaftClient();
        RaftState.getInstance().raftClient(raftClient);
        raftServer = new RaftServer(current, raftClient);
        raftServer.setHandler(new RaftServerHandler(raftServer, raftClient));
        raftServer.setClientHandler(new RaftClientHandler(raftServer, raftClient));
        engineExecutor = new EngineExecutor(raftServer);
    }

    public void run() {
        Executor.start(raftServer);
        Executor.start(raftClient);
        Executor.start(engineExecutor);
    }

    public void shutdown() {
        engineExecutor.shutdown();
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
