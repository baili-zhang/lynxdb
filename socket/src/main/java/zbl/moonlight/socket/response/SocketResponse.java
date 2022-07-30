package zbl.moonlight.socket.response;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.socket.interfaces.SocketBytesConvertible;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static zbl.moonlight.core.utils.NumberUtils.LONG_LENGTH;

public class SocketResponse implements SocketBytesConvertible {
    protected final SelectionKey selectionKey;

    protected long serial;
    protected byte[] data;

    protected SocketResponse(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public long serial() {
        return serial;
    }

    public byte[] data() {
        return data;
    }

    @Override
    public byte[] toContentBytes() {
        ByteBuffer buffer = ByteBuffer.allocate(LONG_LENGTH + data.length);
        return buffer.putLong(serial).put(data).array();
    }

    @Override
    public void fromBytes(byte[] bytes) {
        EnhanceByteBuffer buffer = EnhanceByteBuffer.wrap(bytes);
        serial = buffer.getLong();
        data = buffer.getBytes();
    }
}
