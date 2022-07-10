package zbl.moonlight.socket.request;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.socket.interfaces.Writable;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WritableSocketRequest extends SocketRequest implements Writable {
    private final ByteBuffer buffer;

    public WritableSocketRequest(SelectionKey selectionKey, byte status, long serial, byte[] data) {
        super(selectionKey);
        buffer = ByteBuffer.wrap(build(status, serial, data));
    }

    public WritableSocketRequest(SelectionKey selectionKey, byte[] bytes) {
        super(selectionKey);
        buffer = ByteBuffer.wrap(build(bytes));
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
        return EnhanceByteBuffer.isOver(buffer);
    }

    private byte[] build(byte status, long serial, byte[] data) {
        int length = NumberUtils.BYTE_LENGTH + NumberUtils.LONG_LENGTH + NumberUtils.INT_LENGTH + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        byte[] bytes = buffer.put(status).putLong(serial).putInt(data.length).put(data).array();
        return build(bytes);
    }

    private byte[] build(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH + bytes.length);
        return buffer.putInt(bytes.length).put(bytes).array();
    }
}
