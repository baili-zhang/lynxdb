package zbl.moonlight.socket.response;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.socket.interfaces.Readable;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.LONG_LENGTH;

public class ReadableSocketResponse extends SocketResponse implements Readable {
    private final ByteBuffer lengthBuffer;
    private final ByteBuffer serialBuffer;
    private ByteBuffer dataBuffer;


    public ReadableSocketResponse(SelectionKey key) {
        super(key);
        lengthBuffer = ByteBuffer.allocate(INT_LENGTH);
        serialBuffer = ByteBuffer.allocate(LONG_LENGTH);
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
            dataBuffer = ByteBuffer.allocate(len - LONG_LENGTH);
        }

        if(!EnhanceByteBuffer.isOver(serialBuffer)) {
            channel.read(serialBuffer);
            if(!EnhanceByteBuffer.isOver(serialBuffer)) {
                return;
            }
            serial = serialBuffer.getLong(0);
        }

        if(!EnhanceByteBuffer.isOver(dataBuffer)) {
            channel.read(dataBuffer);
            if(EnhanceByteBuffer.isOver(dataBuffer)) {
                data = dataBuffer.array();
            }
        }
    }

    @Override
    public boolean isReadCompleted() {
        return EnhanceByteBuffer.isOver(dataBuffer);
    }
}
