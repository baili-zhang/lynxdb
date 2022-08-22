package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.*;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.core.SingleTableRow;
import zbl.moonlight.storage.rocks.query.Query;

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
