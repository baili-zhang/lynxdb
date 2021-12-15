package zbl.moonlight.server.engine.rbtree;

import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.engine.Cacheable;

import java.nio.ByteBuffer;

public class RbTreeCache implements Cacheable {
    public void set(String key, ByteBuffer value) {

    }

    public ByteBuffer get(String key) {
        return null;
    }

    public void update(String key, ByteBuffer value) {

    }

    public void delete(String key) {

    }

    @Override
    public void exec(Command command) {
    }
}
