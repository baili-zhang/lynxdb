package zbl.moonlight.server.storage.concrete;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.rocksdb.Snapshot;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class RocksDbEngineTest {
    private byte[] key = "key".getBytes(StandardCharsets.UTF_8);
    private byte[] value = "value".getBytes(StandardCharsets.UTF_8);


    private RocksDbEngine engine;

    @BeforeEach
    void beforeEach() {
        engine = new RocksDbEngine();
    }

    @Test
    void test() {
        assert engine.get(key) == null;
        engine.set(key, value);
        assert Arrays.equals(engine.get(key), value);
        engine.delete(key);
        assert engine.get(key) == null;
    }
}