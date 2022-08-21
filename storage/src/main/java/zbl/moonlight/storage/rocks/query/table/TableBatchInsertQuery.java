package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.*;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.Key;
import zbl.moonlight.storage.core.MultiTableRows;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.core.exception.ColumnsNotExistedException;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.HashSet;
import java.util.Map;

public class TableBatchInsertQuery extends Query<MultiTableRows, Void> {
    public TableBatchInsertQuery(MultiTableRows queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions writeOptions = new WriteOptions()) {

            ColumnsNotExistedException exception = new ColumnsNotExistedException();
            HashSet<Column> columns = new HashSet<>();

            for(ColumnFamilyHandle handle : columnFamilyHandles) {
                Column column = new Column(handle.getName());
                columns.add(column);
            }

            for(Map<Column, byte[]> row : queryData.values()) {
                for(Column column : row.keySet()) {
                    if(!columns.contains(column)) {
                        exception.addColumn(column);
                    }
                }
            }

            if(exception.isNotEmpty()) {
                throw exception;
            }

            for(Key key : queryData.keySet()) {
                Map<Column, byte[]> row = queryData.get(key);
                for(ColumnFamilyHandle handle : columnFamilyHandles) {
                    Column column = new Column(handle.getName());
                    if(row.containsKey(column)) {
                        writeBatch.put(handle, key.value(), row.get(column));
                    }
                }
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
