package zbl.moonlight.server.config;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.server.context.Configuration;

import java.io.IOException;

class ConfigurationTest {
    @Test
    void testLoads() throws IOException {
        Configuration config = Configuration.getInstance();

        ServerNode node = config.currentNode();
        assert node.equals(new ServerNode("127.0.0.1", 7820));
    }
}