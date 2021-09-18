package moonlight.reactor;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;

public class Dispatcher {
    private volatile Selector selector;
    private HashMap<SelectionKey, EventHandler> keyHandlerMap;
    private Set<SelectionKey> handlingEvent;

    private static Dispatcher dispatcher;

    private Dispatcher(Selector selector) {
        this.selector = selector;
        keyHandlerMap = new HashMap<>();
        handlingEvent = new HashSet<>();
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
                selector.wakeup();
                break;
            case WRITE_EVENT:
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
                break;
            case TIMEOUT_EVENT:

        }
    }

    public void removeHandler(EventHandler eventHandler, EventType eventType) {
        eventHandler.eventType = null;
        keyHandlerMap.remove(eventHandler.getSelectionKey());
    }

    public void registerHandlingEvent(SelectionKey selectionKey) {
        handlingEvent.add(selectionKey);
    }

    public void removeHandlingEvent(SelectionKey selectionKey) {
        handlingEvent.remove(selectionKey);
    }

    public void handleEvents() throws IOException {
        selector.select();
        Set<SelectionKey> selectionKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectionKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey selectionKey = iterator.next();
            EventHandler eventHandler = keyHandlerMap.get(selectionKey);

            System.out.println(eventHandler);

            if(eventHandler != null && !handlingEvent.contains(eventHandler.getSelectionKey())) {
                WorkerPool.getInstance().execute(eventHandler);
                registerHandlingEvent(eventHandler.getSelectionKey());
            }
            iterator.remove();
        }
    }
}
