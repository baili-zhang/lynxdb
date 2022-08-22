package zbl.moonlight.socket.request;

import zbl.moonlight.core.common.BytesConvertible;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.socket.interfaces.Writable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

public class WritableSocketRequest extends SocketRequest
        implements Writable, BytesConvertible {

    private final ByteBuffer buffer;

    public WritableSocketRequest(SelectionKey selectionKey, byte status, int serial, byte[] data) {
        super(selectionKey);

        this.status = status;
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

    @Override
    public byte[] toBytes() {
        int length = INT_LENGTH * 2 + BYTE_LENGTH + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);

        return buffer.putInt(data.length).put(status)
                .putInt(serial).put(data).array();
    }
}
