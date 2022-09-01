package com.bailizhang.lynxdb.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;

import java.util.Arrays;

public class TableSingleDeleteColumnQuery extends Query<byte[], Void> {
    public TableSingleDeleteColumnQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            if(Arrays.equals(queryData, handle.getName())) {
                db.dropColumnFamily(handle);
            }
        }
    }
}
