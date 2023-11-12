package com.bailizhang.lynxdb.table.columnfamily;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.LsmTree;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ColumnFamilyRegion {
    public final static String COLUMNS_DIR = "columns";

    private final HashMap<String, LsmTree> columnRegions = new HashMap<>();

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
                        new LsmTree(columnFamily, column, options)
                )
        );
    }

    public LsmTree findColumnRegion(String column) {
        return columnRegions.computeIfAbsent(
                column,
                c -> new LsmTree(
                        columnFamily,
                        c,
                        options
                )
        );
    }

    public HashMap<String, byte[]> findMultiColumns(byte[] key, String... findColumns) {
        HashMap<String, byte[]> multiColumns = new HashMap<>();

        Collection<LsmTree> findColumnRegions;
        if(findColumns == null || findColumns.length == 0) {
            findColumnRegions = columnRegions.values();
        } else {
            findColumnRegions = new ArrayList<>();
            for(String findColumn : findColumns) {
                LsmTree columnRegion = columnRegions.get(findColumn);
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
        Collection<LsmTree> deleteColumnRegions;

        if(deleteColumns == null || deleteColumns.length == 0) {
            deleteColumnRegions = columnRegions.values();
        } else {
            deleteColumnRegions = new ArrayList<>();
            for(String deleteColumn : deleteColumns) {
                LsmTree columnRegion = columnRegions.get(deleteColumn);
                deleteColumnRegions.add(columnRegion);
            }
        }

        deleteColumnRegions.forEach(region -> region.delete(key));
    }
}
