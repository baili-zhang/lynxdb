package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.protocol.Mdtp;
import zbl.moonlight.server.response.Response;
import zbl.moonlight.server.response.ResponseCode;
import zbl.moonlight.server.engine.Engine;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache extends Engine {
    private ConcurrentHashMap<ByteBuffer, DynamicByteBuffer> cache = new ConcurrentHashMap<>();

    @Override
    protected void set(Mdtp mdtp) {
        cache.put(mdtp.getKey(), mdtp.getValue());
        mdtp.setResponse(new Response(ResponseCode.SUCCESS_NO_VALUE));
    }

    @Override
    protected void get(Mdtp mdtp) {
        DynamicByteBuffer value = cache.get(mdtp.getKey());
        Response response = new Response();
        if(value == null) {
            response.setStatus(ResponseCode.VALUE_NOT_EXIST);
        } else {
            response.setStatus(ResponseCode.VALUE_EXIST);
            response.setValue(value);
        }
        mdtp.setResponse(response);
    }

    @Override
    protected void update(Mdtp mdtp) {
    }

    @Override
    protected void delete(Mdtp mdtp) {
    }

}
