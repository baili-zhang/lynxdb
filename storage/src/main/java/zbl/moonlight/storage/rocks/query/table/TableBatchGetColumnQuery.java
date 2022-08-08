package zbl.moonlight.storage.rocks.query.table;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.ArrayList;
import java.util.List;

/**
 * 获取当前表的所有 column
 */
public class TableBatchGetColumnQuery extends Query<Void, List<byte[]>> {
    public TableBatchGetColumnQuery(ResultSet<List<byte[]>> resultSet) {
        super(null, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<byte[]> result = new ArrayList<>();

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            result.add(handle.getName());
        }

        resultSet.setResult(result);
    }
}
