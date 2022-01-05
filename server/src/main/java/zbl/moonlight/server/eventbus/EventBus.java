package zbl.moonlight.server.eventbus;

import lombok.Getter;
import zbl.moonlight.server.executor.Executable;

import java.util.concurrent.ConcurrentHashMap;

public class EventBus implements Executable<Event<?>> {
    @Getter
    private final String NAME = "EventBus";

    private final ConcurrentHashMap<EventType, Dispatcher> dispatchers
            = new ConcurrentHashMap<>();

    public EventBus() {

    }

    public void register(EventType type, Executable<Event<?>> executor, Thread notifiedThread) {
        Dispatcher dispatcher = dispatchers.get(type);
        if(dispatcher == null) {
            dispatcher = new Dispatcher();
            dispatchers.put(type, dispatcher);
        }
        dispatcher.register(executor, notifiedThread);
    }

    @Override
    public void run() {

    }

    @Override
    public void offer(Event event) {

    }

    /* 启动所有注册的线程 */
    public void start() {

    }
}
