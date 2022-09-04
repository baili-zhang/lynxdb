package com.bailizhang.lynxdb.storage.rocks;

import org.rocksdb.*;
import com.bailizhang.lynxdb.storage.core.Database;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDatabase implements Database {
    public static final String COLUMN_FAMILY_ALREADY_EXISTS = "Column family already exists";

    private final Options options;

    private final DBOptions dbOptions;

    private final RocksDB rocksDB;

    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final ColumnFamilyHandle defaultHandle;

    static {
        RocksDB.loadLibrary();
    }

    private RocksDatabase(String path) throws RocksDBException {
        options = new Options();
        dbOptions = new DBOptions().setCreateIfMissing(true);

        List<byte[]> cfs = RocksDB.listColumnFamilies(options, path);
        if(cfs.isEmpty()) {
            cfs = List.of(RocksDB.DEFAULT_COLUMN_FAMILY);
        }

        final List<ColumnFamilyDescriptor> cfDescriptors = cfs.stream()
                .map(ColumnFamilyDescriptor::new).toList();

        rocksDB = RocksDB.open(dbOptions, path, cfDescriptors, columnFamilyHandles);

        ColumnFamilyHandle defaultHandle = null;
        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            if(Arrays.equals(handle.getName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
                defaultHandle = handle;
            }
        }

        if(defaultHandle == null) {
            throw new RuntimeException("Default column family handle is null.");
        }

        this.defaultHandle = defaultHandle;
    }

    @Override
    public void close() throws Exception {
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            columnFamilyHandle.close();
        }

        rocksDB.close();
        dbOptions.close();
        options.close();
    }

    public static RocksDatabase open(String path) throws RocksDBException {
        return new RocksDatabase(path);
    }

    @Override
    public synchronized ResultSet<?> doQuery(Query<?, ?> query) throws RocksDBException {
        query.setDefaultHandle(defaultHandle);
        query.setColumnFamilyHandles(columnFamilyHandles);
        query.doQuery(rocksDB);
        return query.resultSet();
    }
}