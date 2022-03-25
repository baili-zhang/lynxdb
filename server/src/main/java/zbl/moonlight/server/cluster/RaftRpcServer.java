package zbl.moonlight.server.cluster;

import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.core.protocol.common.Readable;
import zbl.moonlight.core.socket.SocketServer;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.EventBus;

public class RaftRpcServer extends SocketServer {
    /** Raft Rpc 的默认端口号 */
    private final static int DEFAULT_PORT = 7850;
    private final static EventBus eventBus = ServerContext.getInstance().getEventBus();
    private final static String ioThreadNamePrefix = "Rpc-Server-IO-";

    /** Raft Rpc 的端口号 */
    private final static int port = DEFAULT_PORT;

    public RaftRpcServer() {
        super(15, 30, 30, 2000,
                port, 20, eventBus, EventType.RAFT_REQUEST, ioThreadNamePrefix, null);
    }
}
