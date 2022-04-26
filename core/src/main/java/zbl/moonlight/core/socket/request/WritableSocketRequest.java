package zbl.moonlight.core.socket.request;

import lombok.Getter;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;

public class WritableSocketRequest implements Writable {
    @Getter
    private final boolean broadcast;
    @Getter
    private final ServerNode serverNode;
    private ByteBuffer buffer;

    public WritableSocketRequest(boolean broadcast, byte[] data, ServerNode serverNode) {
        this.broadcast = broadcast;
        this.serverNode = serverNode;
    }

    @Override
    public void write() throws IOException {

    }

    @Override
    public boolean isWriteCompleted() {
        return false;
    }
}
