package zbl.moonlight.server.engine.simple;

import lombok.Getter;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;
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
    public MdtpResponse doSet(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        ByteBuffer value = mdtpRequest.getValue();
        key.rewind();
        value.rewind();

        cache.put(key, value);

        response.setSuccessNoValue();
        return response;
    }

    @MethodMapping(MdtpMethod.GET)
    public MdtpResponse doGet(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        key.rewind();

        ByteBuffer value = cache.get(key);

        if(value == null) {
            response.setValueNotExist();
            return response;
        }

        response.setValue(value.asReadOnlyBuffer());
        response.setValueExist();
        return response;
    }

    @MethodMapping(MdtpMethod.DELETE)
    public MdtpResponse doDelete(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        key.rewind();

        cache.remove(key);

        response.setSuccessNoValue();
        return response;
    }

    @MethodMapping(MdtpMethod.PING)
    public MdtpResponse doPing(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());
        response.setValue(ByteBuffer.wrap("PONG".getBytes(StandardCharsets.UTF_8)));
        response.setValueExist();
        return response;
    }
}
