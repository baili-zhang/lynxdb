package com.bailizhang.lynxdb.storage.core;

import java.util.HashSet;
import java.util.List;

public class MultiTableKeys extends Pair<List<byte[]>, HashSet<Column>> {
    public MultiTableKeys(List<byte[]> keys, HashSet<Column> columns) {
        super(keys, columns);
    }
}
