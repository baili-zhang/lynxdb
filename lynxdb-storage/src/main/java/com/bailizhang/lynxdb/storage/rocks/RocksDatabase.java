package com.bailizhang.lynxdb.storage.rocks;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.storage.core.Database;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.core.Snapshot;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksDatabase implements Database {
    public static final String COLUMN_FAMILY_ALREADY_EXISTS = "Column family already exists";

    private final String name;

    private final Options options;
    private final DBOptions dbOptions;
    private final RocksDB rocksDB;

    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final ColumnFamilyHandle defaultHandle;

    static {
        RocksDB.loadLibrary();
    }

    private RocksDatabase(String dir, String dbname) throws RocksDBException {
        String path = Path.of(dir, dbname).toString();
        name = dbname;

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

    public static RocksDatabase open(String dir, String dbname) throws RocksDBException {
        return new RocksDatabase(dir, dbname);
    }

    @Override
    public synchronized ResultSet<?> doQuery(Query<?, ?> query) throws RocksDBException {
        query.setDefaultHandle(defaultHandle);
        query.setColumnFamilyHandles(columnFamilyHandles);
        query.doQuery(rocksDB);
        return query.resultSet();
    }

    @Override
    public Snapshot snapshot() {
        BytesList bytesList = new BytesList();

        try (ReadOptions readOptions = new ReadOptions()) {
            readOptions.setSnapshot(rocksDB.getSnapshot());

            for(ColumnFamilyHandle handle : columnFamilyHandles) {
                byte[] column = handle.getName();
                bytesList.appendVarBytes(column);

                try(RocksIterator itr = rocksDB.newIterator(handle, readOptions)) {
                    for(itr.seekToFirst(); itr.isValid(); itr.next()) {
                        byte[] key = itr.key();
                        byte[] value = itr.value();

                        bytesList.appendVarBytes(key);
                        bytesList.appendVarBytes(value);
                    }
                }
            }
        } catch (RocksDBException e) {
            e.printStackTrace();
        }

        return new RocksSnapshot(name, bytesList);
    }

}
