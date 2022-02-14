package zbl.moonlight.server.engine.real;

import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

public class RealCacheEngine extends Engine {
    protected RealCacheEngine(EventBus eventBus) {
        super(eventBus);
    }

    @MethodMapping(MdtpMethod.SET)
    protected MdtpResponse set(MdtpRequest mdtpRequest) {
        return null;
    }

    @MethodMapping(MdtpMethod.GET)
    protected MdtpResponse get(MdtpRequest mdtpRequest) {
        return null;
    }

    @MethodMapping(MdtpMethod.DELETE)
    protected MdtpResponse delete(MdtpRequest mdtpRequest) {
        return null;
    }
}
