package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.MdtpRequest;

/* 注册[事件处理模块]和提交[事件] */
public class EventBus {

    private final Registry registry = new Registry();
    private final Dispatcher dispatcher = new Dispatcher();

    public EventBus() {
    }

    public void register(Subscriber subscriber) {
        this.registry.bind(subscriber);
    }

    public void unregister(Subscriber subscriber) {
        this.registry.unbind(subscriber);
    }

    public void post(MdtpRequest mdtpRequest) {
        this.dispatcher.dispatch(registry, mdtpRequest);
    }
}
