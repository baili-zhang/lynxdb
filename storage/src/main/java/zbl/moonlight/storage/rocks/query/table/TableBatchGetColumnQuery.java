package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.Column;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
