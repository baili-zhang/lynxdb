package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.MdtpRequest;

import java.util.concurrent.ConcurrentLinkedQueue;

/* 把[事件]放入对应的[入口队列]中，并同时[事件处理模块]处理事件 */
public class Dispatcher {
    public void dispatch(Registry registry, MdtpRequest request) {
        ConcurrentLinkedQueue<Subscriber> subscribers = registry.getSubscribers();
        subscribers.stream().forEach(subscriber -> subscriber.handle(request));
    }
}
