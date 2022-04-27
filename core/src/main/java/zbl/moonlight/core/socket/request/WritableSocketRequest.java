package zbl.moonlight.core.socket.request;

import lombok.Getter;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.Writable;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketRequest implements Writable {
    @Getter
    private final ServerNode serverNode;
    private final ByteBuffer buffer;
    private final SelectionKey selectionKey;

    public WritableSocketRequest(SocketRequest request, SelectionKey key) {
        serverNode = (ServerNode) key.attachment();
        selectionKey = key;

        byte[] data = request.data();
        byte status = request.status();
        buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH + NumberUtils.BYTE_LENGTH + data.length);
        buffer.putInt(data.length).put(status).put(data);
        buffer.rewind();
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
