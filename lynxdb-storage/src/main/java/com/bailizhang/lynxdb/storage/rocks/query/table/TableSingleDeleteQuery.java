package com.bailizhang.lynxdb.storage.rocks.query.table;

import org.rocksdb.*;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;

public class TableSingleDeleteQuery extends Query<byte[], Void> {
    public TableSingleDeleteQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions writeOptions = new WriteOptions()) {

            for (ColumnFamilyHandle handle : columnFamilyHandles) {
                writeBatch.delete(handle, queryData);
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
