/*
 * Copyright 2022-2024 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.table.region;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.TableOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.LsmTree;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

public class ColumnFamilyRegion {
    private final HashMap<String, ColumnRegion> columnRegions = new HashMap<>();

    private final String columnFamily;
    private final TableOptions options;

    public ColumnFamilyRegion(String columnFamily, TableOptions options) {
        this.columnFamily = columnFamily;
        this.options = options;

        String baseDir = options.baseDir();
        String dir = Path.of(baseDir, columnFamily).toString();

        List<String> columns = FileUtils.findSubDirs(dir);
        columns.forEach(
                column -> columnRegions.put(
                        column,
                        new ColumnRegion(columnFamily, column, options)
                )
        );
    }

    public LsmTree findColumnRegion(String column) {
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
