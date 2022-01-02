package zbl.moonlight.server.eventbus;

import zbl.moonlight.server.protocol.MdtpRequest;

@FunctionalInterface
public interface Subscriber {
    void handle (MdtpRequest request);
}
