package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

public class SkipList {
    public static final int MAX_LEVEL = 32;

    private final SkipListNode head;
    private int size;

    public SkipList() {
        head = new SkipListNode(BufferUtils.EMPTY_BYTES, BufferUtils.EMPTY_BYTES, MAX_LEVEL);
    }

    public int size() {
        return size;
    }

    public void insert(byte[] key, byte[] column, long timestamp, byte[] value) {
        if(ByteArrayUtils.isEmpty(key) || ByteArrayUtils.isEmpty(column)) {
            throw new RuntimeException();
        }

        size ++;

        SkipListNode node = new SkipListNode(key, column);
        SkipListNode[] prev = new SkipListNode[MAX_LEVEL];

        for(int i = MAX_LEVEL - 1; i >= 0; i --) {
            prev[i] = i == MAX_LEVEL - 1 ? head : prev[i+1];
            SkipListNode next = prev[i].next()[i];

            while(next != null && next.compareTo(node) <= 0) {
                prev[i] = next;
                next = prev[i].next()[i];
            }

            if(prev[i].compareTo(node) == 0) {
                prev[i].insertValue(timestamp, value);
                return;
            }
        }

        for(int i = node.next().length - 1; i >= 0; i --) {
            SkipListNode next = prev[i].next()[i];
            prev[i].next()[i] = node;
            node.next()[i] = next;
        }

        node.insertValue(timestamp, value);
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        if(ByteArrayUtils.isEmpty(key) || ByteArrayUtils.isEmpty(column)) {
            throw new RuntimeException();
        }

        SkipListNode node = new SkipListNode(key, column);
        SkipListNode[] prev = new SkipListNode[MAX_LEVEL];

        for(int i = MAX_LEVEL - 1; i >= 0; i --) {
            prev[i] = i == MAX_LEVEL - 1 ? head : prev[i+1];
            SkipListNode next = prev[i].next()[i];

            while(next != null && next.compareTo(node) <= 0) {
                prev[i] = next;
                next = prev[i].next()[i];
            }

            if(prev[i].compareTo(node) == 0) {
                return prev[i].findValue(timestamp);
            }
        }

        return null;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        if(ByteArrayUtils.isEmpty(key) || ByteArrayUtils.isEmpty(column)) {
            throw new RuntimeException();
        }

        SkipListNode node = new SkipListNode(key, column);
        SkipListNode[] prev = new SkipListNode[MAX_LEVEL];

        for(int i = MAX_LEVEL - 1; i >= 0; i --) {
            prev[i] = i == MAX_LEVEL - 1 ? head : prev[i+1];
            SkipListNode next = prev[i].next()[i];

            while(next != null && next.compareTo(node) <= 0) {
                prev[i] = next;
                next = prev[i].next()[i];
            }

            if(prev[i].compareTo(node) == 0) {
                return prev[i].deleteValue(timestamp);
            }
        }

        return false;
    }
}
