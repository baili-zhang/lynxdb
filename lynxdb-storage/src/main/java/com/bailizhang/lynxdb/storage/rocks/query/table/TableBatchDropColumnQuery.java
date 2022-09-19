package com.bailizhang.lynxdb.storage.rocks.query.table;

import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TableBatchDropColumnQuery extends Query<HashSet<Column>, Void> {
    public TableBatchDropColumnQuery(HashSet<Column> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            if(queryData.contains(new Column(handle.getName()))) {
                cfHandles.add(handle);
            }
        }

        cfHandles.forEach(handle -> columnFamilyHandles.remove(handle));
        db.dropColumnFamilies(cfHandles);
        cfHandles.forEach(ColumnFamilyHandle::close);
    }
}
