package com.bailizhang.lynxdb.storage.rocks.query.table;

import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.HashSet;

/**
 * 获取当前表的所有 column
 */
public class TableBatchGetColumnQuery extends Query<Void, HashSet<Column>> {
    public TableBatchGetColumnQuery(ResultSet<HashSet<Column>> resultSet) {
        super(null, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        HashSet<Column> result = new HashSet<>();

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            result.add(new Column(handle.getName()));
        }

        resultSet.setResult(result);
    }
}
