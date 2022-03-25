package zbl.moonlight.server.cluster;

import zbl.moonlight.core.executor.Executor;

public class RaftRpcServer extends Executor {
    /* Raft Rpc 的默认端口号 */
    private final int DEFAULT_PORT = 7850;
    /* Raft Rpc 的端口号 */
    private final int port = DEFAULT_PORT;

    @Override
    public void run() {

    }
}
