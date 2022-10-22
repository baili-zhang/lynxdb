package com.bailizhang.lynxdb.storage.rocks.query.table;

import com.bailizhang.lynxdb.storage.core.Column;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.core.SingleTableKey;
import com.bailizhang.lynxdb.storage.core.SingleTableRow;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.Set;

public class TableSingleGetQuery extends Query<SingleTableKey, SingleTableRow> {
    public TableSingleGetQuery(SingleTableKey queryData, ResultSet<SingleTableRow> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        byte[] key = queryData.left();
        Set<Column> columns = queryData.right();

        SingleTableRow row = new SingleTableRow(key);

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            Column column = new Column(handle.getName());

            if(columns.contains(column)) {
                byte[] value = db.get(handle, key);
                row.put(column, value);
            }
        }

        resultSet.setResult(row);
    }
}
