package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.schema.Column;

import java.util.HashMap;

public class ColumnFamilyRegion {
    private final HashMap<Column, ColumnRegion> cfRegions = new HashMap<>();

    private final String columnFamily;
    private final LsmTreeOptions options;

    public ColumnFamilyRegion(String columnFamily, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.options = options;
    }

    public ColumnRegion findColumnRegion(byte[] col) {
        Column column = new Column(col);
        return cfRegions.computeIfAbsent(
                column,
                c -> new ColumnRegion(
                        columnFamily,
                        G.I.toString(c.bytes()),
                        options
                )
        );
    }
}
