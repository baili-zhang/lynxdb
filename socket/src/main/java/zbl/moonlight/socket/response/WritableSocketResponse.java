package zbl.moonlight.socket.response;

import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketResponse extends SocketResponse implements Writable {
    private final ByteBuffer buffer;

    public WritableSocketResponse(SelectionKey selectionKey, long serial, byte[] data) {
        super(selectionKey);
        this.serial = serial;
        this.data = data;

        buffer = ByteBuffer.wrap(toBytes());
    }

    @Override
    public void write() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!isWriteCompleted()) {
            channel.write(buffer);
        }
    }

    @Override
    public boolean isWriteCompleted() {
        return BufferUtils.isOver(buffer);
    }
}
