package zbl.moonlight.core.raft.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class RaftServerTest {
    @Test
    void start() throws IOException {
        RaftServer server = new RaftServer((entries) -> {
            // apply entries to state machine
        });
        server.start();
    }
}