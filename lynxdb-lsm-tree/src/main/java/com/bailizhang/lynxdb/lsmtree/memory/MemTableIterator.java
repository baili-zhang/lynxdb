package com.bailizhang.lynxdb.lsmtree.memory;

import java.util.Iterator;

public class MemTableIterator implements Iterator<SkipListNode> {
    private SkipListNode ptr;

    public MemTableIterator(SkipListNode head) {
        ptr = head.next()[0];
    }

    @Override
    public boolean hasNext() {
        return ptr.next()[0] != null;
    }

    @Override
    public SkipListNode next() {
        ptr = ptr.next()[0];
        return ptr;
    }
}
