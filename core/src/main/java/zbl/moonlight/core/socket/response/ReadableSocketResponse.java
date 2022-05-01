package zbl.moonlight.core.socket.response;

import lombok.Getter;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.Readable;
import zbl.moonlight.core.utils.ByteBufferUtils;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadableSocketResponse implements Readable {
    private final Object attachment;

    @Getter
    private final ServerNode serverNode;
    private final ByteBuffer length;
    private final SelectionKey selectionKey;
    private ByteBuffer data;

    public ReadableSocketResponse(SelectionKey key, Object attachment) {
        this.attachment = attachment;
        serverNode = (ServerNode) key.attachment();
        selectionKey = key;
        length = ByteBuffer.allocate(NumberUtils.INT_LENGTH);
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!ByteBufferUtils.isOver(length)) {
            channel.read(length);
            if(!ByteBufferUtils.isOver(length)) {
                return;
            }
            int len = length.getInt(0);
            data = ByteBuffer.allocate(len);
        }

        if(!ByteBufferUtils.isOver(data)) {
            channel.read(data);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return ByteBufferUtils.isOver(data);
    }

    public SocketResponse socketResponse() {
        if(!isReadCompleted()) {
            throw new RuntimeException("Can not get socket response when read is not completed.");
        }
        return new SocketResponse(selectionKey, data.array(), attachment);
    }
}
