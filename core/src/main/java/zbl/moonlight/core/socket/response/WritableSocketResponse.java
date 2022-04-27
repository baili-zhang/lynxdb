package zbl.moonlight.core.socket.response;

import zbl.moonlight.core.socket.interfaces.Writable;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketResponse implements Writable {
    private final SelectionKey selectionKey;
    private final ByteBuffer buffer;

    public WritableSocketResponse(SelectionKey key, byte[] data) {
        selectionKey = key;
        buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH + data.length);
        buffer.putInt(data.length).put(data);
        buffer.rewind();
    }

    public WritableSocketResponse(SocketResponse response) {
        this(response.selectionKey(), response.data());
    }

    public SelectionKey selectionKey() {
        return selectionKey;
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
        return ByteBufferUtils.isOver(buffer);
    }
}
