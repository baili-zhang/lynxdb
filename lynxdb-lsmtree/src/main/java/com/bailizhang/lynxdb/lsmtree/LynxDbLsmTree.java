package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion;
import com.bailizhang.lynxdb.lsmtree.schema.ColumnFamily;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbLsmTree implements LsmTree {
    private final Options options;
    private final String baseDir;

    private final ConcurrentHashMap<ColumnFamily, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbLsmTree(String dir, Options options) {
        baseDir = dir;
        this.options = options;

        FileUtils.createDirIfNotExisted(dir);

        List<String> subDirs = FileUtils.findSubDirs(dir);

        for(String subDir : subDirs) {
            ColumnFamily cf = new ColumnFamily(G.I.toBytes(subDir));
            ColumnFamilyRegion region = new ColumnFamilyRegion(dir, subDir, this.options);
            regions.put(cf, region);
        }
    }

    @Override
    public byte[] find(byte[] key, byte[] columnFamily, byte[] column) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        DbKey dbKey = new DbKey(key, column, DbKey.EXISTED);

        try {
            return region.find(dbKey);
        } catch (DeletedException ignore) {
            return null;
        }
    }

    @Override
    public List<DbValue> find(byte[] key, byte[] columnFamily) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        return region.find(key);
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, byte[] column,
                       byte[] value) {
        ColumnFamilyRegion region = findRegion(columnFamily);

        DbKey dbKey = new DbKey(key, column, DbKey.EXISTED);
        DbEntry dbEntry = new DbEntry(dbKey, value);
        region.insert(dbEntry);
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, List<DbValue> dbValues) {
        ColumnFamilyRegion region = findRegion(columnFamily);

        dbValues.forEach(dbValue -> {
            byte[] column = dbValue.column();
            byte[] value = dbValue.value();

            DbKey dbKey = new DbKey(key, column, DbKey.EXISTED);
            DbEntry dbEntry = new DbEntry(dbKey, value);
            region.insert(dbEntry);
        });
    }

    @Override
    public void delete(byte[] key, byte[] columnFamily, byte[] column) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        DbKey dbKey = new DbKey(key, column, DbKey.DELETED);
        region.delete(dbKey);
    }

    @Override
    public void delete(byte[] key, byte[] columnFamily) {
        // TODO: 只查询 key，不查询 value
        List<DbValue> dbValues = find(key, columnFamily);

        dbValues.forEach(dbValue -> {
            byte[] column = dbValue.column();
            delete(key, columnFamily, column);
        });
    }

    @Override
    public void clear() {
        FileUtils.delete(Path.of(baseDir));
    }

    private ColumnFamilyRegion findRegion(byte[] columnFamily) {
        ColumnFamily cf = new ColumnFamily(columnFamily);
        ColumnFamilyRegion region = regions.get(cf);

        if(region == null) {
            region = new ColumnFamilyRegion(baseDir, G.I.toString(columnFamily), options);
            regions.put(cf, region);
        }

        return region;
    }
}
