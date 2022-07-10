package zbl.moonlight.storage.core;

import java.nio.channels.SelectionKey;

public abstract class AbstractNioQuery implements Queryable {
    protected SelectionKey selectionKey;

    protected AbstractNioQuery(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    public SelectionKey selectionKey() {
        return selectionKey;
    }
}
