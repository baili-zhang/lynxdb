package zbl.moonlight.storage.query.cf;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.ColumnFamilyTuple;
import zbl.moonlight.storage.core.ResultSet;

import java.util.HashMap;
import java.util.List;

public class CfDeleteQuery extends CfQuery {

    public CfDeleteQuery(List<ColumnFamilyTuple> tuples) {
        super(tuples);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException {
        HashMap<ColumnFamily, ColumnFamilyTuple> map = new HashMap<>();
        tuples.forEach(tuple -> map.put(tuple.columnFamily(), tuple));

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            ColumnFamilyTuple tuple = map.get(new ColumnFamily(handle.getName()));
            if(tuple == null) {
                continue;
            }
            db.delete(handle, tuple.keyBytes());
        }
    }
}
