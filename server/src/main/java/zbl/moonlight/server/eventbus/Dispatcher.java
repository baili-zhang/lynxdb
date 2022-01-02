package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.MdtpRequest;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Dispatcher {
    public void dispatch(Registry registry, MdtpRequest request) {
        ConcurrentLinkedQueue<Subscriber> subscribers = registry.getSubscribers();
        subscribers.stream().forEach(subscriber -> subscriber.handle(request));
    }
}
