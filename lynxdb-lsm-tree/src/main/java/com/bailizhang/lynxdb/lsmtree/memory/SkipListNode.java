package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.*;

import static com.bailizhang.lynxdb.lsmtree.common.Version.LATEST_VERSION;
import static com.bailizhang.lynxdb.lsmtree.memory.SkipList.MAX_LEVEL;

public class SkipListNode implements Comparable<SkipListNode> {
    private final byte[] key;
    private final byte[] column;
    private final Deque<VersionalValue> values = new LinkedList<>();
    private final SkipListNode[] next;

    public SkipListNode(byte[] keyBytes, byte[] columnBytes, int level) {
        next = new SkipListNode[level];
        key = keyBytes;
        column = columnBytes;
    }

    public byte[] key() {
        return key;
    }

    public byte[] column() {
        return column;
    }

    public Deque<VersionalValue> values() {
        return values;
    }

    public SkipListNode(byte[] key, byte[] column) {
        this(key, column, random());
    }

    public SkipListNode[] next() {
        return next;
    }

    public void insertValue(long timestamp, byte[] value) {
        values.offerLast(new VersionalValue(timestamp, value));
    }

    public byte[] findValue(long timestamp) {
        if(timestamp == LATEST_VERSION) {
            VersionalValue node = values.peekLast();
            return node == null ? null : node.value();
        }

        Optional<VersionalValue> optional = values.stream()
                .filter(node -> node.timestamp() == timestamp)
                .findFirst();

        return optional.map(VersionalValue::value).orElse(null);
    }

    public boolean deleteValue(long timestamp) {
        return values.removeIf(node -> node.timestamp() == timestamp);
    }

    @Override
    public int compareTo(SkipListNode o) {
        if(Arrays.equals(key, o.key)) {
            return ByteArrayUtils.compare(column, o.column);
        }
        return ByteArrayUtils.compare(key, o.key);
    }

    private static int random() {
        Random random = new Random();
        int level = 1;
        for(int i = 1; i < MAX_LEVEL; i ++) {
            if(random.nextBoolean()) {
                level ++;
            } else {
                break;
            }
        }

        return level;
    }
}
