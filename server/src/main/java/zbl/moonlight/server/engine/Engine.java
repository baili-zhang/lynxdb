package zbl.moonlight.server.engine;

import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpResponse;

public abstract class Engine {
    public final MdtpResponse exec(MdtpRequest mdtpRequest) {
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

    protected abstract MdtpResponse set(MdtpRequest mdtpRequest);
    protected abstract MdtpResponse get(MdtpRequest mdtpRequest);
    protected abstract MdtpResponse delete(MdtpRequest mdtpRequest);
}
