package com.bailizhang.lynxdb.storage.rocks.query.table;

import org.rocksdb.*;
import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.core.SingleTableRow;
import com.bailizhang.lynxdb.storage.rocks.query.Query;

public class TableSingleSetQuery extends Query<SingleTableRow, Void> {
    public TableSingleSetQuery(SingleTableRow queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions writeOptions = new WriteOptions()) {

            for(ColumnFamilyHandle handle : columnFamilyHandles) {
                byte[] key = queryData.rowKey();
                Column column = new Column(handle.getName());
                if(queryData.containsKey(column)) {
                    writeBatch.put(handle, key, queryData.get(column));
                }
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
