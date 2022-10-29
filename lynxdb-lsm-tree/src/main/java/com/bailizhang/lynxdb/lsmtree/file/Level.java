package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

public class Level {
    public void merge(MemTable immutable) {
    }

    public void merge(Level level) {
    }

    public boolean isNotFull() {
        return false;
    }

    public boolean isFull() {
        return true;
    }
}
