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

public class TableBatchDropColumnQuery extends Query<HashSet<Column>, Void> {
    public TableBatchDropColumnQuery(HashSet<Column> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            if(queryData.contains(new Column(handle.getName()))) {
                cfHandles.add(handle);
            }
        }

        cfHandles.forEach(handle -> columnFamilyHandles.remove(handle));
        db.dropColumnFamilies(cfHandles);
        cfHandles.forEach(ColumnFamilyHandle::close);
    }
}
