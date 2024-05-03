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

package com.bailizhang.lynxdb.table;

import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.common.Tuple;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.table.config.TableOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.LsmTree;
import com.bailizhang.lynxdb.table.region.ColumnFamilyRegion;
import com.bailizhang.lynxdb.table.region.ColumnRegion;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbTable implements Table {
    private final TableOptions options;
    private final String baseDir;

    private final ConcurrentHashMap<String, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbTable(TableOptions options) {
        baseDir = options.baseDir();
        this.options = options;

        FileUtils.createDirIfNotExisted(baseDir);

        List<String> subDirs = FileUtils.findSubDirs(baseDir);

        for(String columnFamily : subDirs) {
            ColumnFamilyRegion region = new ColumnFamilyRegion(columnFamily, this.options);
            regions.put(columnFamily, region);
        }
    }

    @Override
    public byte[] find(byte[] key, String columnFamily, String column) {
        ColumnFamilyRegion cfRegion = findColumnFamilyRegion(columnFamily);
        LsmTree columnRegion = cfRegion.findColumnRegion(column);

        try {
            return columnRegion.find(key);
        } catch (DeletedException | TimeoutException o_0) {
            return null;
        }
    }

    @Override
    public HashMap<String, byte[]> findMultiColumns(byte[] key, String columnFamily, String... findColumns) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        return region.findMultiColumns(key, findColumns);
    }

    @Override
    public List<Pair<byte[], HashMap<String, byte[]>>> rangeNext(
            String columnFamily,
            String mainColumn,
            byte[] beginKey,
            int limit,
            String... findColumns
    ) {
        return range(
                columnFamily,
                mainColumn,
                beginKey,
                limit,
                ColumnRegion::rangeNext,
                findColumns
        );
    }

    @Override
    public List<Pair<byte[], HashMap<String, byte[]>>> rangeBefore(
            String columnFamily,
            String mainColumn,
            byte[] endKey,
            int limit,
            String... findColumns
    ) {
        return range(
                columnFamily,
                mainColumn,
                endKey,
                limit,
                ColumnRegion::rangeBefore,
                findColumns
        );
    }

    @Override
    public void insert(
            String columnFamily,
            String column,
            List<Tuple<byte[], byte[], Long>> kvPairs
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        LsmTree columnRegion = region.findColumnRegion(column);

        for(var pair : kvPairs) {
            columnRegion.insert(pair.first(), pair.second(), pair.third());
        }
    }

    @Override
    public void insert(
            byte[] key,
            String columnFamily, HashMap<String, byte[]> multiColumns,
            long timeout
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);

        multiColumns.forEach((column, value) -> {
            LsmTree columnRegion = region.findColumnRegion(column);
            columnRegion.insert(key, value, timeout);
        });
    }

    @Override
    public boolean insertIfNotExisted(
            byte[] key,
            String columnFamily,
            HashMap<String, byte[]> multiColumns,
            long timeout
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);

        for(String column : multiColumns.keySet()) {
            LsmTree columnRegion = region.findColumnRegion(column);
            if(columnRegion.existKey(key)) {
                return false;
            }
        }

        multiColumns.forEach((column, value) -> {
            LsmTree columnRegion = region.findColumnRegion(column);
            columnRegion.insert(key, value, timeout);
        });

        return true;
    }

    @Override
    public void delete(byte[] key, String columnFamily, String column) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        LsmTree columnRegion = region.findColumnRegion(column);
        columnRegion.delete(key);
    }

    @Override
    public void deleteMultiColumns(byte[] key, String columnFamily, String... deleteColumns) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        region.deleteMultiColumns(key, deleteColumns);
    }

    @Override
    public boolean existKey(byte[] key, String columnFamily, String mainColumn) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        LsmTree columnRegion = region.findColumnRegion(mainColumn);
        return columnRegion.existKey(key);
    }

    @Override
    public void clear() {
        FileUtils.delete(Path.of(baseDir));
    }

    private ColumnFamilyRegion findColumnFamilyRegion(String columnFamily) {
        return regions.computeIfAbsent(
                columnFamily,
                v -> new ColumnFamilyRegion(columnFamily, options)
        );
    }

    private List<Pair<byte[], HashMap<String, byte[]>>> range(
            String columnFamily,
            String mainColumn,
            byte[] baseKey,
            int limit,
            RangeOperator operator,
            String... findColumns
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        ColumnRegion mainColumnRegion = region.findColumnRegion(mainColumn);

        List<byte[]> keys = operator.doRange(mainColumnRegion, baseKey, limit);

        List<Pair<byte[], HashMap<String, byte[]>>> values = new ArrayList<>();

        for(byte[] key : keys) {
            HashMap<String, byte[]> multiColumns = region.findMultiColumns(key, findColumns);
            values.add(new Pair<>(key, multiColumns));
        }

        return values;
    }

    @FunctionalInterface
    private interface RangeOperator {
        List<byte[]> doRange(
                ColumnRegion columnRegion,
                byte[] baseKey,
                int limit
        );
    }
}
