package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.KeyEntry;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion;
import com.bailizhang.lynxdb.lsmtree.file.ColumnRegion;
import com.bailizhang.lynxdb.lsmtree.schema.ColumnFamily;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbLsmTree implements Table {
    private final LsmTreeOptions options;
    private final String baseDir;

    private final ConcurrentHashMap<ColumnFamily, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbLsmTree(String dir, LsmTreeOptions options) {
        baseDir = dir;
        this.options = options;

        FileUtils.createDirIfNotExisted(dir);

        List<String> subDirs = FileUtils.findSubDirs(dir);

        for(String subDir : subDirs) {
            ColumnFamily cf = new ColumnFamily(G.I.toBytes(subDir));
            ColumnFamilyRegion region = new ColumnFamilyRegion(subDir, this.options);
            regions.put(cf, region);
        }
    }

    @Override
    public byte[] find(byte[] key, byte[] columnFamily, byte[] column) {
        ColumnFamilyRegion cfRegion = findColumnFamilyRegion(columnFamily);
        ColumnRegion columnRegion = cfRegion.findColumnRegion(column);

        return columnRegion.find(key);
    }

    @Override
    public HashMap<byte[], byte[]> find(byte[] key, byte[] columnFamily) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        return region.find(key);
    }

    @Override
    public HashMap<byte[], HashMap<byte[], byte[]>> findAll(byte[] columnFamily) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        return region.findAll();
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, byte[] column,
                       byte[] value) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);

        KeyEntry dbKey = new KeyEntry(key, column, KeyEntry.EXISTED);
        DbEntry dbEntry = new DbEntry(dbKey, value);
        region.insert(dbEntry);
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, HashMap<byte[], byte[]> multiColumns) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);

        dbValues.forEach(dbValue -> {
            byte[] column = dbValue.column();
            byte[] value = dbValue.value();

            KeyEntry dbKey = new KeyEntry(key, column, KeyEntry.EXISTED);
            DbEntry dbEntry = new DbEntry(dbKey, value);
            region.insert(dbEntry);
        });
    }

    @Override
    public void delete(byte[] key, byte[] columnFamily, byte[] column) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        KeyEntry dbKey = new KeyEntry(key, column, KeyEntry.DELETED);
        region.delete(dbKey);
    }

    @Override
    public void delete(byte[] key, byte[] columnFamily) {
        // TODO: 只查询 dbKey，不查询 value
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        List<byte[]> columns = region.findColumns(key);

        columns.forEach(column -> delete(key, columnFamily, column));
    }

    @Override
    public boolean existKey(byte[] key, byte[] columnFamily) {
        ColumnFamilyRegion region = findColumnFamilyRegion(columnFamily);
        return region.existKey(key);
    }

    @Override
    public void clear() {
        FileUtils.delete(Path.of(baseDir));
    }

    private ColumnFamilyRegion findColumnFamilyRegion(byte[] columnFamily) {
        ColumnFamily cf = new ColumnFamily(columnFamily);
        ColumnFamilyRegion region = regions.get(cf);

        if(region == null) {
            region = new ColumnFamilyRegion(G.I.toString(columnFamily), options);
            regions.put(cf, region);
        }

        return region;
    }
}
