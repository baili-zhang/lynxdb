package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.MultiTableKeys;
import zbl.moonlight.storage.core.MultiTableRows;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableBatchGetQuery extends Query<MultiTableKeys, MultiTableRows> {
    public TableBatchGetQuery(MultiTableKeys queryData, ResultSet<MultiTableRows> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<byte[]> keys = queryData.left();
        HashSet<Column> columns = queryData.right();

        MultiTableRows result = new MultiTableRows();

        for(byte[] key : keys) {
            Map<Column, byte[]> row = new LinkedHashMap<>();

            for(ColumnFamilyHandle handle : columnFamilyHandles) {
                Column column = new Column(handle.getName());
                if(columns.contains(column)) {
                    byte[] value = db.get(handle, key);
                    row.put(column, value);
                }
            }

            result.put(key, row);
        }
    }
}
