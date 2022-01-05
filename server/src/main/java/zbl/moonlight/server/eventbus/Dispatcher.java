package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.executor.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Dispatcher {
    private final ConcurrentLinkedQueue<Event<?>> queue;
    private final List<Map.Entry<Executable<Event<?>>, Thread>> subscribers;

    public Dispatcher() {
        queue = new ConcurrentLinkedQueue<>();
        subscribers = new ArrayList<>();
    }

    public void register(Executable<Event<?>> executor, Thread notifiedThread) {
        subscribers.add(Map.entry(executor, notifiedThread));
    }
}
