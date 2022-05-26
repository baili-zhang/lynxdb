package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.server.RaftServer;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.storage.StorageEngine;
import zbl.moonlight.server.mdtp.MdtpStateMachine;

import java.io.IOException;
import java.util.HashMap;

public class MoonlightServer {
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private final RaftServer raftServer;
    private final StorageEngine storageEngine;

    MoonlightServer() throws IOException {
        Configuration config = new Configuration();

        ServerNode current = config.currentNode();
        String logFilenamePrefix = current.host() + "_" + current.port() + "_raft_";

        MdtpStateMachine stateMachine = new MdtpStateMachine();
        raftServer = new RaftServer(stateMachine, null,
                null, logFilenamePrefix);
        storageEngine = new StorageEngine(raftServer.socketServer(), new HashMap<>());
        stateMachine.setStorageEngine(storageEngine);
    }

    public void run() {
        raftServer.start();
        Executor.start(storageEngine);
    }

    public void shutdown() {
        storageEngine.shutdown();
    }

    public static void main(String[] args) throws IOException {
        new MoonlightServer().run();
    }
}
