package zbl.moonlight.server.engine;

import zbl.moonlight.server.command.Command;

import java.nio.ByteBuffer;

public interface Cacheable {
    public void exec(Command command);
}
