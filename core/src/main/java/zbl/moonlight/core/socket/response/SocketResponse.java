package zbl.moonlight.core.socket.response;

import zbl.moonlight.core.raft.response.BytesConvertable;

import java.nio.channels.SelectionKey;

public abstract class SocketResponse implements BytesConvertable {
    protected final SelectionKey selectionKey;

    protected SocketResponse(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }
}
