package com.bailizhang.lynxdb.table.lsmtree.sstable;

import java.nio.ByteBuffer;

public record SecondIndexRegion(
        ByteBuffer buffer,
        int keyAmount
) {
}
