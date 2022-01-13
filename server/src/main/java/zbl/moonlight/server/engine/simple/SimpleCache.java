package zbl.moonlight.server.engine.simple;

import lombok.Getter;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;
import zbl.moonlight.server.engine.Engine;

import java.nio.ByteBuffer;

public class SimpleCache extends Engine {
    @Getter
    private final String NAME = "SimpleCache";

    private LRU<ByteBuffer, ByteBuffer> cache
            = new LRU<>(900000);

    public SimpleCache(EventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected MdtpResponse set(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        ByteBuffer value = mdtpRequest.getValue();
        key.flip();
        value.flip();

        cache.put(key, value);

        response.setSuccessNoValue();
        return response;
    }

    @Override
    protected MdtpResponse get(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        key.flip();

        ByteBuffer value = cache.get(key);

        if(value == null) {
            response.setValueNotExist();
            return response;
        }

        response.setValue(value.asReadOnlyBuffer());
        response.setValueExist();
        return response;
    }

    @Override
    protected MdtpResponse delete(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());

        ByteBuffer key = mdtpRequest.getKey();
        key.flip();

        cache.remove(key);

        response.setSuccessNoValue();
        return response;
    }
}
