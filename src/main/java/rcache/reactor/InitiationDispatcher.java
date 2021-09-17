package rcache.reactor;

import java.util.List;

public class InitiationDispatcher {
    private SynchronousEventDemultiplexer demultiplexer;

    public InitiationDispatcher(SynchronousEventDemultiplexer demultiplexer) {
        this.demultiplexer = demultiplexer;
    }
    public void registerHandler(EventHandler eventHandler, EventType eventType) {
    }

    public void removeHandler(EventHandler eventHandler, EventType eventType) {

    }

    public void handleEvents() {
        List<Handle> handles = demultiplexer.select();

        for (Handle handle: handles) {
            handle.getEventHandler().handleEvent();
        }
    }
}
