package com.bailizhang.lynxdb.table.lsmtree.sstable;

import java.nio.ByteBuffer;

public record SecondIndexRegion(
        int firstIndexEntryIdx,
        ByteBuffer buffer,
        int keyAmount
) {
}
