package zbl.moonlight.core.raft.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.server.RaftServer;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RaftServerTest {
    private static final Logger logger = LogManager.getLogger("RaftServerTest");

    private final List<ServerNode> nodes = new ArrayList<>();

    private RaftServerTest prepare() {
        for(int i = 0; i < 5; i ++) {
            nodes.add(new ServerNode("127.0.0.1", 7820 + i));
        }
        return this;
    }

    private void start() throws IOException {
        int count = 1;
        for(ServerNode node : nodes) {
            new RaftServer((entries) -> {
                for (Entry entry : entries) {
                    System.out.println(entry);
                }
            }, node, nodes, "raft_server_" + count)
                    .start("RaftServer-" + count ++);
        }
    }

    public static void main(String[] args) throws IOException {
        new RaftServerTest().prepare().start();
        logger.info("Test for RaftServer started up.");
    }
}
