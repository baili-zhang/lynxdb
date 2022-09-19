package zbl.moonlight.server.config;

import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;

class ConfigurationTest {
    @Test
    void testLoads() throws IOException {
        Configuration config = Configuration.getInstance();

        ServerNode node = config.currentNode();
        assert node.equals(new ServerNode("127.0.0.1", 7820));
    }
}