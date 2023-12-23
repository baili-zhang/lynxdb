package com.bailizhang.lynxdb.table.lsmtree.sstable;

import java.nio.ByteBuffer;
import java.util.Arrays;

public record FirstIndexEntry(
        byte[] beginKey,
        int idx
) implements Comparable<FirstIndexEntry> {
    public static FirstIndexEntry from(ByteBuffer buffer) {
        return new FirstIndexEntry(null, 0);
    }

    @Override
    public int compareTo(FirstIndexEntry o) {
        return Arrays.compare(beginKey, o.beginKey);
    }
}
