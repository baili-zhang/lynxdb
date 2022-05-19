package zbl.moonlight.server.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.lsm.LsmTree;
import zbl.moonlight.core.raft.response.RaftResponse;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.server.mdtp.MdtpCommand;

import java.util.Map;

import static zbl.moonlight.server.mdtp.MdtpCommand.*;

public class StorageEngine extends Executor<MdtpCommand> {
    private static final Logger logger = LogManager.getLogger("Engine");

    private final SocketServer socketServer;
    private final Map<String, byte[]> storage;
    /* TODO: Cache 以后再实现 */

    /* 是否关闭 */
    private boolean shutdown = false;

    public StorageEngine(SocketServer socketServer, Map<String, byte[]> storage) {
        this.socketServer = socketServer;
        this.storage = storage;
    }

    public void shutdown () {
        shutdown = true;
    }

    @Override
    public final void run() {
        while (!shutdown) {
            MdtpCommand command = blockPoll();
            SocketResponse response = exec(command);
            socketServer.offerInterruptibly(response);
        }
    }

    private SocketResponse exec(MdtpCommand command) {
        switch (command.method()) {
            case SET -> { return doSet(command); }
            case GET -> { return doGet(command); }
            case DELETE -> { return doDelete(command); }
            default -> {
                throw new RuntimeException("Unsupported method.");
            }
        }
    }

    private SocketResponse doSet(MdtpCommand command) {
        storage.put(command.key(), command.value());
        byte[] data = RaftResponse.clientRequestSuccessWithoutResult();
        return new SocketResponse(command.selectionKey(), data, null);
    }

    private SocketResponse doGet(MdtpCommand command) {
        byte[] value = storage.get(command.key());
        return null;
    }

    private SocketResponse doDelete(MdtpCommand command) {
        storage.remove(command.key());
        return null;
    }
}
