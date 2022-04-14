package zbl.moonlight.server.raft;

import lombok.Getter;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;

import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class RaftRpcClientContext {
    private boolean connected;
    private ConcurrentLinkedQueue<NioWriter> writers;
    private NioReader reader;

    public RaftRpcClientContext(boolean connected, ConcurrentLinkedQueue<NioWriter> writers, NioReader reader) {
        this.connected = connected;
        this.writers = writers;
        this.reader = reader;
    }
}
