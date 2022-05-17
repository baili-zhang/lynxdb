package zbl.moonlight.core.raft.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.server.RaftServer;
import zbl.moonlight.core.raft.state.StateMachine;
import zbl.moonlight.core.socket.client.ServerNode;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;

public class RaftCluster {
    private static final Logger logger = LogManager.getLogger("RaftServerTest");

    private final List<ServerNode> nodes = new ArrayList<>();

    private RaftCluster prepare() {
        for(int i = 0; i < 7; i ++) {
            nodes.add(new ServerNode("127.0.0.1", 7820 + i));
        }
        return this;
    }

    static class SimpleStateMachine implements StateMachine {
        @Override
        public void apply(Entry[] entries) {
            for (Entry entry : entries) {
                System.out.println("StateMachine Apply: " + new String(entry.command()));
            }
        }

        @Override
        public void exec(SelectionKey key, byte[] command) {
            System.out.println(new String(command));
        }
    }

    private void start() throws IOException {
        int count = 1;
        for(ServerNode node : nodes) {
            new RaftServer(new SimpleStateMachine(),
                    node,nodes, "raft_server_" + count)
                    .start("RaftServer-" + count ++);
        }
    }

    public static void main(String[] args) throws IOException {
        new RaftCluster().prepare().start();
        logger.info("Test for RaftServer started up.");
    }
}
