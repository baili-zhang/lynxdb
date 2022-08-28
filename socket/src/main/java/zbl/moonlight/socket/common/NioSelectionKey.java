package zbl.moonlight.socket.common;

import java.nio.channels.SelectionKey;

public abstract class NioSelectionKey {
    protected final SelectionKey selectionKey;

    public NioSelectionKey(SelectionKey key) {
        selectionKey = key;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }
}
