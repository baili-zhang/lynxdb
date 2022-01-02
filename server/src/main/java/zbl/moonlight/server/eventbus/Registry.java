package zbl.moonlight.server.eventbus;

import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Registry {
    @Getter
    private final ConcurrentLinkedQueue<Subscriber> subscribers = new ConcurrentLinkedQueue<>();

    public void bind(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    public void unbind(Object subscriber) {
        subscribers.remove(subscriber);
    }
}
