package zbl.moonlight.server.storage;

public interface EngineInterface {
    byte[] get(byte[] key);
    void set(byte[] key, byte[] value);
    void delete(byte[] key);
}
