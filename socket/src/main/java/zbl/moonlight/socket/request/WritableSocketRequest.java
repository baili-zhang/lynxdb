package zbl.moonlight.socket.request;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.socket.common.NioSelectionKey;
import zbl.moonlight.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketRequest extends NioSelectionKey implements Writable {
    private final ByteBuffer buffer;

    public WritableSocketRequest(SelectionKey key, byte status,
                                 int serial, byte[] data) {
        super(key);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(status);
        bytesList.appendRawInt(serial);
        bytesList.appendRawBytes(data);

        buffer = ByteBuffer.wrap(bytesList.toBytes());
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
