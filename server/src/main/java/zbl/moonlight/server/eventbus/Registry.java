package zbl.moonlight.server.eventbus;

import lombok.Getter;

import java.util.concurrent.ConcurrentLinkedQueue;

/* 注册[事件名]和[入口队列列表] */
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
