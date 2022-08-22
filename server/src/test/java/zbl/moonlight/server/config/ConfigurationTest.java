package zbl.moonlight.server.config;

import org.junit.jupiter.api.Test;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;

class ConfigurationTest {
    @Test
    void testLoads() throws IOException {
        Configuration config = Configuration.getInstance();

        ServerNode node = config.currentNode();
        assert node.equals(new ServerNode("127.0.0.1", 7820));
    }
}