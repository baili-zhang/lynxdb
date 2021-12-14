package zbl.moonlight.server.command;

import lombok.Data;
import zbl.moonlight.server.engine.Cacheable;
import zbl.moonlight.server.engine.simple.SimpleCache;

import java.nio.ByteBuffer;

@Data
public abstract class Command implements Cloneable {
    private static Cacheable cache;

    static {
        cache = SimpleCache.getInstance();
    }

    public static Cacheable getCache() {
        return cache;
    }

    private String key;
    private ByteBuffer value;
    private boolean isKeyExisted;

    @Override
    public Command clone() throws CloneNotSupportedException {
        return (Command) super.clone();
    }

    public abstract Command exec();
    public abstract String wrap();
}
