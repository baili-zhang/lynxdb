package com.bailizhang.lynxdb.table.region;

import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.lsmtree.LsmTree;

public class ColumnRegion extends LsmTree {
    private final String columnFamily;
    private final String column;

    public ColumnRegion(String columnFamily, String column, LsmTreeOptions options) {
        super(options);
        this.columnFamily = columnFamily;
        this.column = column;
    }

    public String columnFamily() {
        return columnFamily;
    }

    public String column() {
        return column;
    }
}
