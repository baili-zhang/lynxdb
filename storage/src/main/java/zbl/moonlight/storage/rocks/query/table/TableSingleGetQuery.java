package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.core.SingleTableKey;
import zbl.moonlight.storage.core.SingleTableRow;
import zbl.moonlight.storage.rocks.query.Query;

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
