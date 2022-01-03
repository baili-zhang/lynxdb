package zbl.moonlight.server.log;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

class LogTest {

    @Test
    void append() throws IOException {
        Log log = new Log();
        log.append(ByteBuffer.wrap("hallo-world".getBytes(StandardCharsets.UTF_8)));
    }
}