package com.bailizhang.lynxdb.lsmtree.memory;

import java.util.Iterator;

public class MemTable implements Iterable<SkipListNode> {
    private static final int DEFAULT_MAX_SIZE = 2000;

    private volatile boolean immutable = false;
    private final SkipList skipList = new SkipList();

    public void append(byte[] key, byte[] column, long timestamp, byte[] value) {
        if(immutable) {
            return;
        }

        skipList.insert(key, column, timestamp, value);
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        return skipList.find(key, column, timestamp);
    }

    public boolean full() {
        return skipList.size() >= DEFAULT_MAX_SIZE;
    }

    public void transformToImmutable() {
        immutable = true;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        return skipList.delete(key, column, timestamp);
    }

    @Override
    public Iterator<SkipListNode> iterator() {
        return skipList.iterator();
    }
}
