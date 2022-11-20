package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.Options;

import java.util.Iterator;

public class MemTable implements Iterable<SkipListNode> {
    private final Options options;
    private volatile boolean immutable = false;
    private final SkipList skipList = new SkipList();

    public MemTable(Options options) {
        this.options = options;
    }

    public void append(DbEntry dbEntry) {
        if(immutable) {
            return;
        }

        DbKey dbKey = dbEntry.key();

        skipList.insert(
                dbKey.key(),
                dbKey.column(),
                dbKey.timestamp(),
                dbEntry.value()
        );
    }

    public byte[] find(DbKey dbKey) {
        return skipList.find(
                dbKey.key(),
                dbKey.column(),
                dbKey.timestamp()
        );
    }

    public boolean full() {
        return skipList.size() >= options.memTableSize();
    }

    public void transformToImmutable() {
        immutable = true;
    }

    public boolean delete(DbKey dbKey) {
        return skipList.delete(
                dbKey.key(),
                dbKey.column(),
                dbKey.timestamp()
        );
    }

    @Override
    public Iterator<SkipListNode> iterator() {
        return skipList.iterator();
    }
}
