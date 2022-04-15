package zbl.moonlight.server.raft;

import lombok.Getter;
import lombok.Setter;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.server.mdtp.MdtpResponseSchema;

import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
public class RaftRpcClientContext {
    private final SelectionKey selectionKey;
    private final ConcurrentLinkedQueue<NioWriter> writers;

    @Setter
    private boolean connected;
    private NioReader reader;

    public RaftRpcClientContext(SelectionKey key) {
        selectionKey = key;
        connected = false;
        writers = new ConcurrentLinkedQueue<>();
        reader = new NioReader(MdtpResponseSchema.class, selectionKey);
    }

    public void newReader() {
        reader = new NioReader(MdtpResponseSchema.class, selectionKey);
    }
}
