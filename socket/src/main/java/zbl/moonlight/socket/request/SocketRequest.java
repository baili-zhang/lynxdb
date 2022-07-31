package zbl.moonlight.socket.request;

import zbl.moonlight.core.enhance.EnhanceByteBuffer;
import zbl.moonlight.core.exceptions.NullFieldException;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.socket.interfaces.SocketBytesConvertible;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public abstract class SocketRequest implements SocketBytesConvertible {
    protected SelectionKey selectionKey;
    protected Byte status;
    protected Integer serial;
    protected byte[] data;

    protected SocketRequest() {}

    public void selectionKey(SelectionKey val) {
        if(selectionKey == null) {
            selectionKey = val;
        }
    }

    public SelectionKey selectionKey() {
        if(selectionKey == null) {
            throw new NullFieldException("selectionKey");
        }
        return selectionKey;
    }

    public void status(byte val) {
        if(status == null) {
            status = val;
        }
    }

    public byte status() {
        if(status == null) {
            throw new NullFieldException("status");
        }
        return status;
    }

    public void serial(int val) {
        if(serial == null) {
            serial = val;
        }
    }

    public int serial() {
        if(serial == null) {
            throw new NullFieldException("serial");
        }
        return serial;
    }

    public void data(byte[] val) {
        if(data == null) {
            data = val;
        }
    }

    public byte[] data() {
        if(data == null) {
            throw new NullFieldException("data");
        }
        return data;
    }

    @Override
    public final byte[] toContentBytes() {
        int length = NumberUtils.BYTE_LENGTH + NumberUtils.LONG_LENGTH + data.length;
        ByteBuffer buffer = ByteBuffer.allocate(length);
        return buffer.put(status).putLong(serial).put(data).array();
    }

    @Override
    public void fromBytes(byte[] bytes) {
        EnhanceByteBuffer buffer = EnhanceByteBuffer.wrap(bytes);
        status = buffer.get();
        serial = buffer.getInt();
        data = buffer.getRemaining();
    }

    public boolean isKeepConnection() {
        return true;
    }

    public boolean isBroadcast() {
        return selectionKey == null;
    }
}
