package zbl.moonlight.socket.request;

import zbl.moonlight.socket.interfaces.SocketBytesConvertible;

import java.nio.channels.SelectionKey;

public class SocketRequest implements SocketBytesConvertible {
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

    }

    @Override
    public void fromBytes(byte[] bytes) {

    }
}
