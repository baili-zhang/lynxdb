package zbl.moonlight.socket.response;

import zbl.moonlight.core.common.BytesConvertible;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

public class SocketResponse {
    protected final SelectionKey selectionKey;

    protected int serial;
    protected byte[] data;

    protected SocketResponse(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }

    public int serial() {
        return serial;
    }

    public byte[] data() {
        return data;
    }
}
