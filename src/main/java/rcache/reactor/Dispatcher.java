package rcache.reactor;

import java.io.IOException;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Dispatcher {
    private Selector selector;
    private HashMap<SelectionKey, EventHandler> keyHandlerMap;

    private static Dispatcher dispatcher;

    private Dispatcher(Selector selector) {
        this.selector = selector;
        keyHandlerMap = new HashMap<>();
    }

    synchronized public static void init(Selector selector) {
        if(dispatcher == null)
            dispatcher = new Dispatcher(selector);
    }

    public static Dispatcher getInstance() throws NullPointerException {
        if(dispatcher == null) {
            throw new NullPointerException("init Dispatcher before getInstance");
        }
        return dispatcher;
    }

    public void registerHandler(EventHandler eventHandler, EventType eventType) {
        SelectionKey key = eventHandler.getSelectionKey();
        eventHandler.eventType = eventType;
        keyHandlerMap.put(key, eventHandler);

        switch (eventType) {
            case READ_EVENT:
                key.interestOps(SelectionKey.OP_READ);
                break;
            case WRITE_EVENT:
                key.interestOps(SelectionKey.OP_WRITE);
                break;
            case TIMEOUT_EVENT:

        }
    }

    public void removeHandler(EventHandler eventHandler, EventType eventType) {
        eventHandler.eventType = null;
        keyHandlerMap.remove(eventHandler.getSelectionKey());
    }

    public void handleEvents() throws IOException {
        selector.select();
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectionKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey selectionKey = iterator.next();
            EventHandler eventHandler = keyHandlerMap.get(selectionKey);
            WorkerPool.getInstance().execute(eventHandler);
            iterator.remove();
        }
    }
}
