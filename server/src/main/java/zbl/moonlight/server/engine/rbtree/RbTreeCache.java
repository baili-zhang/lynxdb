package zbl.moonlight.server.engine.rbtree;

import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.response.Response;

import java.nio.ByteBuffer;

public class RbTreeCache extends Engine {
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
