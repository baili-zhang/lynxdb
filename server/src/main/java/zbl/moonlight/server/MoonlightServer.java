package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.server.RaftServer;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.concrete.RocksDbEngine;
import zbl.moonlight.server.storage.EngineExecutor;
import zbl.moonlight.server.mdtp.MdtpStateMachine;

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
        raftServer = new RaftServer(stateMachine, current,
                config.clusterNodes(), logFilenamePrefix);
        engineExecutor = new EngineExecutor(raftServer.socketServer(), new RocksDbEngine());
        stateMachine.setStorageEngine(engineExecutor);
    }

    public void run() {
        raftServer.start();
        Executor.start(engineExecutor);
    }

    public void shutdown() {
        engineExecutor.shutdown();
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
