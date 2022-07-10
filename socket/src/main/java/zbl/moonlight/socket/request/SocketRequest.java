package zbl.moonlight.socket.request;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.socket.interfaces.SocketBytesConvertible;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public abstract class SocketRequest implements SocketBytesConvertible {
    protected final SelectionKey selectionKey;

    protected byte status;
    protected long serial;
    protected byte[] data;

    SocketRequest(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    @Override
    public final byte[] toContentBytes() {
        int length = NumberUtils.BYTE_LENGTH + NumberUtils.LONG_LENGTH + NumberUtils.INT_LENGTH + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        return buffer.put(status).putLong(serial).putInt(data.length).put(data).array();
    }

    @Override
    public void fromBytes(byte[] bytes) {
        EnhanceByteBuffer buffer = EnhanceByteBuffer.wrap(bytes);
        status = buffer.get();
        serial = buffer.getLong();
        data = buffer.getBytes();
    }

    public boolean isKeepConnection() {
        return false;
    }

    public boolean isBroadcast() {
        return false;
    }
}
