package zbl.moonlight.socket.response;

import zbl.moonlight.socket.interfaces.SocketBytesConvertible;

import java.nio.channels.SelectionKey;

public class AbstractSocketResponse implements SocketBytesConvertible {
    protected final SelectionKey selectionKey;

    protected long serial;
    protected byte[] data;

    protected AbstractSocketResponse(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    @Override
    public byte[] toContentBytes() {
        return new byte[0];
    }

    @Override
    public void fromBytes(byte[] bytes) {

    }
}
