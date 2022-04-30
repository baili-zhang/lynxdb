package zbl.moonlight.core.socket.client;

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class ServerNodeTest {
    @Test
    void testAdd() {
        Set<ServerNode> nodes = new HashSet<>();
        nodes.add(new ServerNode("127.0.0.1", 7820));
        nodes.add(new ServerNode("127.0.0.1", 7820));
        assert nodes.size() == 1;
    }
}