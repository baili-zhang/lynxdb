package zbl.moonlight.server.engine.real;

import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

public class RealCacheEngine extends Engine {
    protected RealCacheEngine(EventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected MdtpResponse set(MdtpRequest mdtpRequest) {
        return null;
    }

    @Override
    protected MdtpResponse get(MdtpRequest mdtpRequest) {
        return null;
    }

    @Override
    protected MdtpResponse delete(MdtpRequest mdtpRequest) {
        return null;
    }
}
