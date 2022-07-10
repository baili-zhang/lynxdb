package zbl.moonlight.socket.response;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.socket.interfaces.Readable;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadableSocketResponse extends SocketResponse implements Readable {
    private final ByteBuffer lengthBuffer;
    private ByteBuffer bytes;


    public ReadableSocketResponse(SelectionKey key) {
        super(key);
        lengthBuffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH);
    }

    @Override
    public void read() throws IOException {
        SocketChannel channel = (SocketChannel) selectionKey.channel();
        if(!EnhanceByteBuffer.isOver(lengthBuffer)) {
            channel.read(lengthBuffer);
            if(!EnhanceByteBuffer.isOver(lengthBuffer)) {
                return;
            }
            int len = lengthBuffer.getInt(0);
            bytes = ByteBuffer.allocate(len);
        }

        if(!EnhanceByteBuffer.isOver(bytes)) {
            channel.read(bytes);
        }
    }

    @Override
    public boolean isReadCompleted() {
        return EnhanceByteBuffer.isOver(bytes);
    }
}
