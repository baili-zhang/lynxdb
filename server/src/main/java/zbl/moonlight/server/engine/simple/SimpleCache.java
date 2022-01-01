package zbl.moonlight.server.engine.simple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;
import zbl.moonlight.server.engine.Engine;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache extends Engine {
    private static final Logger logger = LogManager.getLogger("SimpleCache");

    private ConcurrentHashMap<ByteBuffer, DynamicByteBuffer> cache = new ConcurrentHashMap<>();

    @Override
    protected MdtpResponse set(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse();
        cache.put(mdtpRequest.getKey(), mdtpRequest.getValue());
        response.setSuccessNoValue();
        logger.info("SET method execute.");
        return response;
    }

    @Override
    protected MdtpResponse get(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse();
        DynamicByteBuffer value = cache.get(mdtpRequest.getKey());
        logger.info("GET method execute.");

        if(value == null) {
            response.setValueNotExist();
            return response;
        }
        response.setValue(value);
        response.setValueExist();
        return response;
    }

    @Override
    protected MdtpResponse update(MdtpRequest mdtpRequest) {
        return null;
    }

    @Override
    protected MdtpResponse delete(MdtpRequest mdtpRequest) {
        return null;
    }

}
