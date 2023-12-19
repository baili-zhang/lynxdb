package com.bailizhang.lynxdb.table.lsmtree.sstable;

import java.util.Arrays;

public record FirstIndexEntry(
        byte[] beginKey,
        int idx
) implements Comparable<FirstIndexEntry> {
    @Override
    public int compareTo(FirstIndexEntry o) {
        return Arrays.compare(beginKey, o.beginKey);
    }
}
