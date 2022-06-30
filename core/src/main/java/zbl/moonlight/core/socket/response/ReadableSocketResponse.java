package zbl.moonlight.core.socket.response;

import lombok.Getter;
import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.Readable;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadableSocketResponse implements Readable {
    @Getter
    private final ServerNode serverNode;
    private final ByteBuffer length;
    private final SelectionKey selectionKey;
    private ByteBuffer data;


    public ReadableSocketResponse(SelectionKey key) {
        serverNode = (ServerNode) key.attachment();
        selectionKey = key;
        length = ByteBuffer.allocate(NumberUtils.INT_LENGTH);
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!EnhanceByteBuffer.isOver(length)) {
            channel.read(length);
            if(!EnhanceByteBuffer.isOver(length)) {
                return;
            }
            int len = length.getInt(0);
            data = ByteBuffer.allocate(len);
        }

        if(!EnhanceByteBuffer.isOver(data)) {
            channel.read(data);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return EnhanceByteBuffer.isOver(data);
    }
}
