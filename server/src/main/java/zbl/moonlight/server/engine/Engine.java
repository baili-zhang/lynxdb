package zbl.moonlight.server.engine;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.Method;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.response.Response;

import java.nio.ByteBuffer;

public abstract class Engine {
    public final void exec(Command command) {
        Response response = null;

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

    protected abstract Response set(ByteBuffer key, DynamicByteBuffer value);
    protected abstract Response get(ByteBuffer key);
    protected abstract Response update(ByteBuffer key, DynamicByteBuffer value);
    protected abstract Response delete(ByteBuffer key);
}
