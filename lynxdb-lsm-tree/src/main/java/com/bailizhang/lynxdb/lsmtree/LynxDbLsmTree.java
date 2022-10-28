package com.bailizhang.lynxdb.lsmtree;

import com.bailizhang.lynxdb.lsmtree.common.ColumnFamily;
import com.bailizhang.lynxdb.lsmtree.common.LsmTree;
import com.bailizhang.lynxdb.lsmtree.exception.ColumnFamilyNotFoundException;
import com.bailizhang.lynxdb.lsmtree.file.ColumnFamilyRegion;

import java.util.concurrent.ConcurrentHashMap;

public class LynxDbLsmTree implements LsmTree {
    private final ConcurrentHashMap<ColumnFamily, ColumnFamilyRegion> regions
            = new ConcurrentHashMap<>();

    public LynxDbLsmTree(String dir) {

    }

    @Override
    public byte[] find(byte[] key, byte[] columnFamily, byte[] column, long timestamp)
            throws ColumnFamilyNotFoundException {
        ColumnFamilyRegion region = findRegion(columnFamily);
        return region.find(key, column, timestamp);
    }

    @Override
    public void insert(byte[] key, byte[] columnFamily, byte[] column, long timestamp,
                       byte[] value)
            throws ColumnFamilyNotFoundException {
        ColumnFamilyRegion region = findRegion(columnFamily);
        region.insert(key, column, timestamp, value);
    }

    @Override
    public void delete(byte[] key, byte[] columnFamily, byte[] column, long timestamp)
            throws ColumnFamilyNotFoundException {
        ColumnFamilyRegion region = findRegion(columnFamily);
        region.delete(key, column, timestamp);
    }

    @Override
    public void addColumn(byte[] columnFamily, byte[] column)
            throws ColumnFamilyNotFoundException {
        ColumnFamilyRegion region = findRegion(columnFamily);
        region.addColumn(column);
    }

    @Override
    public void removeColumn(byte[] columnFamily, byte[] column)
            throws ColumnFamilyNotFoundException {
        ColumnFamilyRegion region = findRegion(columnFamily);
        region.removeColumn(column);
    }

    private ColumnFamilyRegion findRegion(byte[] columnFamily)
            throws ColumnFamilyNotFoundException {
        ColumnFamily cf = new ColumnFamily(columnFamily);
        ColumnFamilyRegion region = regions.get(cf);

        if(region == null) {
            throw new ColumnFamilyNotFoundException(columnFamily);
        }

        return region;
    }
}
