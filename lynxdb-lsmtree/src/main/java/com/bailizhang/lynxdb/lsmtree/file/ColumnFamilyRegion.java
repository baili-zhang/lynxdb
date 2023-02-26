package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

public class ColumnFamilyRegion {
    public final static String COLUMNS_DIR = "columns";

    private final HashMap<String, ColumnRegion> columnRegions = new HashMap<>();

    private final String columnFamily;
    private final LsmTreeOptions options;

    public ColumnFamilyRegion(String columnFamily, LsmTreeOptions options) {
        this.columnFamily = columnFamily;
        this.options = options;

        String baseDir = options.baseDir();
        String dir = Path.of(baseDir, columnFamily, COLUMNS_DIR).toString();

        List<String> columns = FileUtils.findSubDirs(dir);
        columns.forEach(
                column -> columnRegions.put(
                        column,
                        new ColumnRegion(columnFamily, column, options)
                )
        );
    }

    public ColumnRegion findColumnRegion(String column) {
        return columnRegions.computeIfAbsent(
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

        columnRegions.forEach((column, columnRegion) -> {
            byte[] value;

            try {
                value = columnRegion.find(key);
                multiColumns.put(column, value);
            } catch (DeletedException ignored) {
            }
        });

        return multiColumns;
    }

    public void deleteAllColumnsByKey(byte[] key) {
        columnRegions.values().forEach(region -> region.delete(key));
    }
}
