package zbl.moonlight.server.engine.simple;

import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;
import zbl.moonlight.server.engine.Engine;

import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache extends Engine {
    private static final Logger logger = LogManager.getLogger("SimpleCache");

    @Getter
    private final String NAME = "SimpleCache";

    private ConcurrentHashMap<String, DynamicByteBuffer> cache
            = new ConcurrentHashMap<>();

    public SimpleCache(EventBus eventBus) {
        super(eventBus);
    }

    @Override
    protected MdtpResponse set(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());
        String key = new String(mdtpRequest.getKey().array());
        cache.put(key, mdtpRequest.getValue());
        response.setSuccessNoValue();
        logger.info("SET method execute, key is: " + key);
        return response;
    }

    @Override
    protected MdtpResponse get(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());
        String key = new String(mdtpRequest.getKey().array());
        DynamicByteBuffer value = cache.get(key);
        logger.info("GET method execute, key is: " + key);

        if(value == null) {
            response.setValueNotExist();
            return response;
        }

        response.setValue(value);
        response.setValueExist();
        return response;
    }

    @Override
    protected MdtpResponse delete(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());
        String key = new String(mdtpRequest.getKey().array());
        cache.remove(key);
        response.setSuccessNoValue();
        logger.info("DELETE method execute.");
        return response;
    }
}
