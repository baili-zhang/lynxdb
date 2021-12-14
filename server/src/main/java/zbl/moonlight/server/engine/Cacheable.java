package zbl.moonlight.server.engine;

import java.nio.ByteBuffer;

public interface Cacheable {
    public void set(String key, ByteBuffer value);
    public ByteBuffer get(String key);
    public void update(String key, ByteBuffer value);
    public void delete(String key);
}
