package zbl.moonlight.server.storage.concrete;

import org.junit.jupiter.api.BeforeEach;
import zbl.moonlight.server.storage.query.*;

import java.nio.charset.StandardCharsets;

class CfDatabaseTest {
    private static final byte[] defaultColumnFamily = "default".getBytes(StandardCharsets.UTF_8);
    private static final byte[] columnFamily = "cf_test".getBytes(StandardCharsets.UTF_8);
    private static final byte[] key = "key_test".getBytes(StandardCharsets.UTF_8);
    private static final byte[] value = "value_test".getBytes(StandardCharsets.UTF_8);

    private CfDatabase cfDatabase;

    @BeforeEach
    void setUp() {
    }
}