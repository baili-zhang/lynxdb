package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.Method;
import zbl.moonlight.server.command.ResponseCode;
import zbl.moonlight.server.engine.Cacheable;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCache implements Cacheable {
    private ConcurrentHashMap<ByteBuffer, ByteBuffer> cache = new ConcurrentHashMap<>();

    public ByteBuffer set(ByteBuffer key, ByteBuffer value) {
        cache.put(key, value);

        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put(ResponseCode.SUCCESS_NO_VALUE);

        return byteBuffer;
    }

    public ByteBuffer get(ByteBuffer key) {
        ByteBuffer value = cache.get(key);

        int capacity = value.capacity();
        ByteBuffer byteBuffer = ByteBuffer.allocate(capacity + 5);

        byteBuffer.put(ResponseCode.VALUE_EXIST);
        byteBuffer.putInt(capacity);
        byteBuffer.put(value);

        return byteBuffer;
    }

    public ByteBuffer update(ByteBuffer key, ByteBuffer value) {
        cache.put(key, value);

        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put(ResponseCode.SUCCESS_NO_VALUE);

        return byteBuffer;
    }

    public ByteBuffer delete(ByteBuffer key) {
        cache.remove(key);

        ByteBuffer byteBuffer = ByteBuffer.allocate(1);
        byteBuffer.put(ResponseCode.SUCCESS_NO_VALUE);

        return byteBuffer;
    }

    @Override
    public void exec(Command command) {
        ByteBuffer response = null;

        switch (command.getCode()) {
            case Method.SET:
                response = set(command.getKey(), command.getValue());
                break;
            case Method.GET:
                response = get(command.getKey());
                break;
            case Method.UPDATE:
                response = update(command.getKey(), command.getValue());
                break;
            case Method.DELETE:
                response = delete(command.getKey());
        }

        command.setResponse(response);
    }
}
