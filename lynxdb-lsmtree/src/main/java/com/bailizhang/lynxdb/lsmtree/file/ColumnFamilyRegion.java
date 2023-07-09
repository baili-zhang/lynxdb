package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.exception.TimeoutException;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
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

    public HashMap<String, byte[]> findMultiColumns(byte[] key, String... findColumns) {
        HashMap<String, byte[]> multiColumns = new HashMap<>();

        Collection<ColumnRegion> findColumnRegions;
        if(findColumns == null || findColumns.length == 0) {
            findColumnRegions = columnRegions.values();
        } else {
            findColumnRegions = new ArrayList<>();
            for(String findColumn : findColumns) {
                ColumnRegion columnRegion = columnRegions.get(findColumn);
                if(columnRegion != null) {
                    findColumnRegions.add(columnRegion);
                }
            }
        }

        findColumnRegions.forEach(columnRegion -> {
            byte[] value;

            try {
                value = columnRegion.find(key);

                if(value == null) {
                    return;
                }

                multiColumns.put(columnRegion.column(), value);
            } catch (DeletedException | TimeoutException ignored) {
            }
        });

        return multiColumns;
    }

    public void deleteMultiColumns(byte[] key, String... deleteColumns) {
        Collection<ColumnRegion> deleteColumnRegions;

        if(deleteColumns == null || deleteColumns.length == 0) {
            deleteColumnRegions = columnRegions.values();
        } else {
            deleteColumnRegions = new ArrayList<>();
            for(String deleteColumn : deleteColumns) {
                ColumnRegion columnRegion = columnRegions.get(deleteColumn);
                deleteColumnRegions.add(columnRegion);
            }
        }

        deleteColumnRegions.forEach(region -> region.delete(key));
    }
}
