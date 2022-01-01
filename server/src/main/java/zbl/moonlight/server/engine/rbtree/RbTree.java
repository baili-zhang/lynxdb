package zbl.moonlight.server.engine.rbtree;

import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;

import java.nio.ByteBuffer;
import java.security.Timestamp;

public class RbTree {

    private Node root;

    public DynamicByteBuffer get(ByteBuffer key) {
        return null;
    }

    public void put(ByteBuffer key, DynamicByteBuffer value) {

    }

    public void remove(ByteBuffer key) {

    }

    private class Node {
        private ByteBuffer key;
        private DynamicByteBuffer value;

        private Node left;
        private Node right;
        private Node parent;

        private Timestamp expires;

        Node(ByteBuffer key, DynamicByteBuffer value) {
            this.key = key;
            this.value = value;
        }
    }
}
