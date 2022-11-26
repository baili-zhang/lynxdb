package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;

import java.util.Iterator;

public class SkipList implements Iterable<SkipListNode> {
    public static final int MAX_LEVEL = 32;

    private final SkipListNode head;
    private int size;

    public SkipList() {
        head = new SkipListNode(BufferUtils.EMPTY_BYTES, BufferUtils.EMPTY_BYTES, null, MAX_LEVEL);
    }

    public int size() {
        return size;
    }

    public void insert(byte[] key, byte[] column, byte[] value) {
        if(ByteArrayUtils.isEmpty(key) || ByteArrayUtils.isEmpty(column)) {
            throw new RuntimeException();
        }

        SkipListNode node = new SkipListNode(key, column, value);
        SkipListNode[] prev = new SkipListNode[MAX_LEVEL];

        for(int i = MAX_LEVEL - 1; i >= 0; i --) {
            prev[i] = i == MAX_LEVEL - 1 ? head : prev[i+1];
            SkipListNode next = prev[i].next()[i];

            while(next != null && next.compareTo(node) <= 0) {
                prev[i] = next;
                next = prev[i].next()[i];
            }

            if(prev[i].compareTo(node) == 0) {
                prev[i].value(value);
                return;
            }
        }

        size ++;

        for(int i = node.next().length - 1; i >= 0; i --) {
            SkipListNode next = prev[i].next()[i];
            prev[i].next()[i] = node;
            node.next()[i] = next;
        }
    }

    public byte[] find(byte[] key, byte[] column) {
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
                return prev[i].value();
            }
        }

        return null;
    }

    public boolean delete(byte[] key, byte[] column) {
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
                return true;
            }
        }

        return false;
    }

    @Override
    public Iterator<SkipListNode> iterator() {
        return new MemTableIterator(head);
    }
}
