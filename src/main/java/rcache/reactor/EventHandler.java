package rcache.reactor;

import java.nio.channels.SelectionKey;

public abstract class EventHandler implements Runnable {
    protected SelectionKey selectionKey;
    protected EventType eventType;

    public EventHandler() {

    }

    public EventHandler(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
    }

    /**
     * handle event
     */
    public abstract void run();

    public SelectionKey getSelectionKey() {
        return selectionKey;
    }
}
