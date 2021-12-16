package zbl.moonlight.server.engine;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.response.Response;

import java.nio.ByteBuffer;

public abstract class Engine {
    public final void exec(Command command) {
        Response response = null;

        switch (command.getCode()) {
            case MdtpMethod.SET:
                response = set(command.getKey(), command.getValue());
                break;
            case MdtpMethod.GET:
                response = get(command.getKey());
                break;
            case MdtpMethod.UPDATE:
                response = update(command.getKey(), command.getValue());
                break;
            case MdtpMethod.DELETE:
                response = delete(command.getKey());
        }

        command.setResponse(response);
    }

    protected abstract Response set(ByteBuffer key, DynamicByteBuffer value);
    protected abstract Response get(ByteBuffer key);
    protected abstract Response update(ByteBuffer key, DynamicByteBuffer value);
    protected abstract Response delete(ByteBuffer key);
}
