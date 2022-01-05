package zbl.moonlight.server.engine;

import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpResponse;

public abstract class Engine extends Executor<Event<?>> {
    protected Engine(EventBus eventBus, Thread eventBusThread) {
        super(eventBus, eventBusThread);
    }

    private final MdtpResponse exec(MdtpRequest mdtpRequest) {
        switch (mdtpRequest.getMethod()) {
            case MdtpMethod.SET:
                return set(mdtpRequest);
            case MdtpMethod.GET:
                return get(mdtpRequest);
            case MdtpMethod.DELETE:
                return delete(mdtpRequest);
        }

        return null;
    }

    @Override
    public final void run() {
        while (true) {
             Event<?> event = pollSleep();
             if(event == null) {
                 continue;
             }
             MdtpRequest request = (MdtpRequest) event.getValue();
             MdtpResponse response = exec(request);
             send(new Event<>(EventType.CLIENT_RESPONSE, event.getSelectionKey(), response));
        }
    }

    protected abstract MdtpResponse set(MdtpRequest mdtpRequest);
    protected abstract MdtpResponse get(MdtpRequest mdtpRequest);
    protected abstract MdtpResponse delete(MdtpRequest mdtpRequest);
}
