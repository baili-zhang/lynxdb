package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.client.RaftClient;
import zbl.moonlight.raft.client.RaftClientHandler;
import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.raft.server.RaftServerHandler;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.server.mdtp.MdtpStateMachine;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final RaftServer raftServer;
    private final EngineExecutor engineExecutor;

    MoonlightServer() throws IOException {
        Configuration config = Configuration.getInstance();
        logger.info("Configuration: [{}]", config);

        ServerNode current = config.currentNode();
        String logFilenamePrefix = current.host() + "_" + current.port() + "_raft_";

        MdtpStateMachine stateMachine = new MdtpStateMachine();
        RaftClient raftClient = new RaftClient();
        raftServer = new RaftServer(stateMachine, current, raftClient, logFilenamePrefix);
        raftServer.setHandler(new RaftServerHandler(raftServer, stateMachine, raftClient, raftServer.raftState()));
        raftServer.setClientHandler(new RaftClientHandler(raftServer.raftState(), raftServer, raftClient));
        engineExecutor = new EngineExecutor(raftServer);
        stateMachine.setStorageEngine(engineExecutor);
    }

    public void run() {
        Executor.start(engineExecutor);
    }

    public void shutdown() {
        engineExecutor.shutdown();
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
