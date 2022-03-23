package zbl.moonlight.server.engine.simple;

import lombok.Getter;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.eventbus.MdtpRequestEvent;
import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.protocol.mdtp.MdtpMethod;
import zbl.moonlight.server.engine.Engine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class SimpleCache extends Engine {
    @Getter
    private final String NAME = "SimpleCache";

    private final SimpleLRU<ByteBuffer, ByteBuffer> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(ServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.SET)
    public MdtpResponseEvent doSet(MdtpRequestEvent mdtpRequest) {
        return null;
    }

    @MethodMapping(MdtpMethod.GET)
    public MdtpResponseEvent doGet(MdtpRequestEvent mdtpRequest) {
        return null;
    }

    @MethodMapping(MdtpMethod.DELETE)
    public MdtpResponseEvent doDelete(MdtpRequestEvent mdtpRequest) {
        return null;
    }

    @MethodMapping(MdtpMethod.PING)
    public MdtpResponseEvent doPing(MdtpRequestEvent mdtpRequest) {
        return null;
    }
}
