package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.memory.SkipListNode;

import java.util.ArrayList;
import java.util.List;

public class Level {
    private static final int LEVEL_SSTABLE_COUNT = 10;

    private final List<SsTable> ssTables = new ArrayList<>(LEVEL_SSTABLE_COUNT);

    public void merge(MemTable immutable) {
        SsTable ssTable = new SsTable();
        for(SkipListNode node : immutable) {
            ssTable.append(node.key(), node.column(), node.values());
        }
    }

    public void merge(Level level) {

    }

    public boolean isNotFull() {
        return !isFull();
    }

    public boolean isFull() {
        return ssTables.size() >= LEVEL_SSTABLE_COUNT;
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        return null;
    }

    public boolean isLessThan(byte[] key, byte[] column) {
        return false;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        return false;
    }
}
