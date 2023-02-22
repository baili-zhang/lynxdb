package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;

import java.util.HashMap;

public class ColumnFamilyRegion {
    private final HashMap<String, ColumnRegion> cfRegions = new HashMap<>();

    private final String columnFamily;
    private final LsmTreeOptions options;

    public ColumnFamilyRegion(String columnFamily, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.options = options;
    }

    public ColumnRegion findColumnRegion(String column) {
        return cfRegions.computeIfAbsent(
                column,
                c -> new ColumnRegion(
                        columnFamily,
                        c,
                        options
                )
        );
    }

    public HashMap<String, byte[]> findAllColumnsByKey(byte[] key) {
        HashMap<String, byte[]> multiColumns = new HashMap<>();

        cfRegions.forEach((column, columnRegion) -> {
            byte[] value;

            try {
                value = columnRegion.find(key);
            } catch (DeletedException ignored) {
                value = null;
            }

            multiColumns.put(column, value);
        });

        return multiColumns;
    }

    public void deleteAllColumnsByKey(byte[] key) {
        cfRegions.values().forEach(region -> region.delete(key));
    }
}
