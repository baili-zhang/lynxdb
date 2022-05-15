package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.server.RaftServer;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.StorageEngine;
import zbl.moonlight.server.exception.ConfigurationException;
import zbl.moonlight.server.mdtp.MdtpStateMachine;

import java.io.IOException;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final Configuration config;
    private final RaftServer raftServer;
    private final StorageEngine storageEngine;

    MoonlightServer() throws ConfigurationException, IOException {
        config = new Configuration();
        raftServer = new RaftServer(new MdtpStateMachine(), config.currentNode(),
                config.getRaftNodes(), "moonlight_" + config.currentNode() + "_raft_");
        storageEngine = new StorageEngine(raftServer.socketServer());
    }

    public void run() {
        raftServer.start();
        Executor.start(storageEngine);
    }

    public void shutdown() {
        storageEngine.shutdown();
    }

    public static void main(String[] args) throws IOException, ConfigurationException {
        new MoonlightServer().run();
    }
}
