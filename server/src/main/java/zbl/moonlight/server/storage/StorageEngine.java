package zbl.moonlight.server.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;

public class StorageEngine extends Executor<byte[]> {
    private static final Logger logger = LogManager.getLogger("Engine");

    private final SocketServer server;

    /* 是否关闭 */
    private boolean shutdown = false;

    public StorageEngine(SocketServer socketServer) {
        server = socketServer;
    }

    public void shutdown () {
        shutdown = true;
    }

    @Override
    public final void run() {
        while (!shutdown) {
            byte[] command = blockPoll();
            SocketResponse response = exec(command);
            server.offerInterruptibly(response);
        }
    }

    private SocketResponse exec(byte[] command) {
        return null;
    }
}
