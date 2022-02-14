package zbl.moonlight.server.engine.real.rbtree;

import java.nio.ByteBuffer;
import java.security.Timestamp;

public class RbTree {

    private Node root;

    public ByteBuffer get(ByteBuffer key) {
        return null;
    }

    public void put(ByteBuffer key, ByteBuffer value) {

    }

    public void remove(ByteBuffer key) {

    }

    private class Node {
        private ByteBuffer key;
        private ByteBuffer value;

        private Node left;
        private Node right;
        private Node parent;

        private Timestamp expires;

        Node(ByteBuffer key, ByteBuffer value) {
            this.key = key;
            this.value = value;
        }
    }
}
