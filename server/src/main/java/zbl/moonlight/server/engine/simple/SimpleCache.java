package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.response.Response;
import zbl.moonlight.server.response.ResponseCode;
import zbl.moonlight.server.engine.Engine;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache extends Engine {
    private ConcurrentHashMap<ByteBuffer, DynamicByteBuffer> cache = new ConcurrentHashMap<>();

    @Override
    protected Response set(ByteBuffer key, DynamicByteBuffer value) {
        return null;
    }

    @Override
    protected Response get(ByteBuffer key) {
        return null;
    }

    @Override
    protected Response update(ByteBuffer key, DynamicByteBuffer value) {
        return null;
    }

    @Override
    protected Response delete(ByteBuffer key) {
        return null;
    }
}
