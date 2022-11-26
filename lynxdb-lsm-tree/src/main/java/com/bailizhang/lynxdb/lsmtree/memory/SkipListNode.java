package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.*;

import static com.bailizhang.lynxdb.lsmtree.memory.SkipList.MAX_LEVEL;

public class SkipListNode implements Comparable<SkipListNode> {
    private final SkipListNode[] next;

    private final byte[] key;
    private final byte[] column;

    private byte[] value;

    public SkipListNode(byte[] key, byte[] column, byte[] value, int level) {
        this.key = key;
        this.column = column;
        this.value = value;

        next = new SkipListNode[level];
    }

    public SkipListNode(byte[] key, byte[] column) {
        this(key, column, null, random());
    }

    public SkipListNode(byte[] key, byte[] column, byte[] value) {
        this(key, column, value, random());
    }

    public byte[] key() {
        return key;
    }

    public byte[] column() {
        return column;
    }

    public void value(byte[] val) {
        value = val;
    }

    public byte[] value() {
        return value;
    }

    public SkipListNode[] next() {
        return next;
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

    @Override
    public int compareTo(SkipListNode o) {
        if(!Arrays.equals(key, o.key)) {
            return ByteArrayUtils.compare(key, o.key);
        }

        return ByteArrayUtils.compare(column, o.column);
    }
}
