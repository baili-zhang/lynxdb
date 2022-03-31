package zbl.moonlight.server.raft;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RaftNodeTest {

    @Test
    void equals() {
        RaftNode n1 = new RaftNode("127.0.0.1", 7820);
        RaftNode n2 = new RaftNode(new String("127.0.0.1"), 7820);
        assert n1.equals(n2);
    }
}