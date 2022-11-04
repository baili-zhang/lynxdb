package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.lsmtree.common.Options;

import java.util.Iterator;

public class MemTable implements Iterable<SkipListNode> {
    private final Options options;
    private volatile boolean immutable = false;
    private final SkipList skipList = new SkipList();

    public MemTable(Options options) {
        this.options = options;
    }

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
        return skipList.size() >= options.memTableSize();
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
