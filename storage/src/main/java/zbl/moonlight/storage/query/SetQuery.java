package zbl.moonlight.storage.query;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.ResultSet;

import java.util.HashMap;
import java.util.List;

public class SetQuery extends Query {
    public SetQuery(List<QueryTuple> tuples) {
        super(tuples);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException {
        HashMap<ColumnFamily, QueryTuple> map = new HashMap<>();
        tuples.forEach(tuple -> map.put(tuple.columnFamily(), tuple));

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            QueryTuple tuple = map.get(new ColumnFamily(handle.getName()));
            if(tuple == null) {
                continue;
            }
            db.put(handle, tuple.keyBytes(), tuple.valueBytes());
        }
    }
}
