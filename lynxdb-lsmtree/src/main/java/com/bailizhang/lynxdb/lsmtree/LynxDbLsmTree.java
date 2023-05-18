package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion;
import com.bailizhang.lynxdb.lsmtree.file.ColumnRegion;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbLsmTree implements Table {
    private final LsmTreeOptions options;
    private final String baseDir;

    private final ConcurrentHashMap<String, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbLsmTree(LsmTreeOptions options) {
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
        ColumnRegion columnRegion = cfRegion.findColumnRegion(column);

        try {
            return columnRegion.find(key);
        } catch (DeletedException ignored) {
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
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        ColumnRegion mainColumnRegion = region.findColumnRegion(mainColumn);

        List<byte[]> keys = mainColumnRegion.rangeNext(beginKey, limit);

        List<Pair<byte[], HashMap<String, byte[]>>> values = new ArrayList<>();

        for(byte[] key : keys) {
            HashMap<String, byte[]> multiColumns = region.findMultiColumns(key, findColumns);
            values.add(new Pair<>(key, multiColumns));
        }

        return values;
    }

    @Override
    public List<Pair<byte[], HashMap<String, byte[]>>> rangeBefore(
            String columnFamily,
            String mainColumn,
            byte[] endKey,
            int limit,
            String... findColumns
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        ColumnRegion mainColumnRegion = region.findColumnRegion(columnFamily);

        List<byte[]> keys = mainColumnRegion.rangeBefore(endKey, limit);

        List<Pair<byte[], HashMap<String, byte[]>>> values = new ArrayList<>();

        for(byte[] key : keys) {
            HashMap<String, byte[]> multiColumns = region.findMultiColumns(key, findColumns);
            values.add(new Pair<>(key, multiColumns));
        }

        return values;
    }

    @Override
    public void insert(
            byte[] key,
            String columnFamily,
            String column,
            byte[] value
    ) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        ColumnRegion columnRegion = region.findColumnRegion(column);
        columnRegion.insert(key, value);
    }

    @Override
    public void insert(byte[] key, String columnFamily, HashMap<String, byte[]> multiColumns) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);

        multiColumns.forEach((column, value) -> {
            ColumnRegion columnRegion = region.findColumnRegion(column);
            columnRegion.insert(key, value);
        });
    }

    @Override
    public void delete(byte[] key, String columnFamily, String column) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        ColumnRegion columnRegion = region.findColumnRegion(column);
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
        ColumnRegion columnRegion = region.findColumnRegion(mainColumn);
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
}
