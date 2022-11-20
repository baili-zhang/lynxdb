package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.*;
import com.bailizhang.lynxdb.lsmtree.exception.ColumnFamilyNotFoundException;
import com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class LynxDbLsmTree implements LsmTree {
    private final Options options;
    private final String baseDir;

    private final ConcurrentHashMap<ColumnFamily, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbLsmTree(String dir, Options configOptions) {
        baseDir = dir;
        options = configOptions;

        FileUtils.createDirIfNotExisted(dir);

        List<String> subDirs = FileUtils.findSubDirs(dir);

        for(String subDir : subDirs) {
            ColumnFamily cf = new ColumnFamily(G.I.toBytes(subDir));
            ColumnFamilyRegion region = new ColumnFamilyRegion(dir, subDir, options);
            regions.put(cf, region);
        }
    }

    @Override
    public byte[] find(byte[] key, byte[] columnFamily, byte[] column, long timestamp) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        DbKey dbKey = new DbKey(key, column, timestamp);
        return region.find(dbKey);
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, byte[] column, long timestamp,
                       byte[] value) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        if(region == null) {
            region = new ColumnFamilyRegion(baseDir, G.I.toString(columnFamily), options);
        }

        DbKey dbKey = new DbKey(key, column, timestamp);
        DbEntry dbEntry = new DbEntry(dbKey, value);
        region.insert(dbEntry);
    }

    @Override
    public boolean delete(byte[] key, byte[] columnFamily, byte[] column, long timestamp) {
        ColumnFamilyRegion region = findRegion(columnFamily);
        DbKey dbKey = new DbKey(key, column, timestamp);
        return region.delete(dbKey);
    }

    private ColumnFamilyRegion findRegion(byte[] columnFamily) {
        ColumnFamily cf = new ColumnFamily(columnFamily);
        ColumnFamilyRegion region = regions.get(cf);

        if(region == null && !options.createColumnFamilyIfNotExisted()) {
            throw new ColumnFamilyNotFoundException(columnFamily);
        }

        return region;
    }
}
