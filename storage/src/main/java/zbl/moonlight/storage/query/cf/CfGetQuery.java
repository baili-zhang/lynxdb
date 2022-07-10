package zbl.moonlight.storage.query.cf;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.*;

import java.util.*;

public class CfGetQuery extends CfQuery {
    public CfGetQuery(List<ColumnFamilyTuple> tuples) {
        super(tuples);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException {
        HashMap<ColumnFamily, ColumnFamilyTuple> map = new HashMap<>();
        tuples.forEach(tuple -> map.put(tuple.columnFamily(), tuple));

        List<ColumnFamilyTuple> result = new ArrayList<>();

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            ColumnFamilyTuple tuple = map.get(new ColumnFamily(handle.getName()));
            if(tuple == null) {
                continue;
            }
            byte[] value = db.get(handle, tuple.keyBytes());
            result.add(new ColumnFamilyTuple(tuple.columnFamily(), tuple.key(), new Value(value)));
        }

        resultSet.setResult(result);
    }
}
