package zbl.moonlight.storage.query;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.*;

import java.util.*;

public class GetQuery extends Query {
    public GetQuery(List<QueryTuple> tuples) {
        super(tuples);
    }

    @Override
    public void doQuery(RocksDB db, ResultSet resultSet) throws RocksDBException {
        HashMap<ColumnFamily, QueryTuple> map = new HashMap<>();
        tuples.forEach(tuple -> map.put(tuple.columnFamily(), tuple));

        List<QueryTuple> result = new ArrayList<>();

        for(ColumnFamilyHandle handle : columnFamilyHandles) {
            QueryTuple tuple = map.get(new ColumnFamily(handle.getName()));
            if(tuple == null) {
                continue;
            }
            byte[] value = db.get(handle, tuple.keyBytes());
            result.add(new QueryTuple(tuple.key(), tuple.columnFamily(), new Value(value)));
        }

        resultSet.setResult(result);
    }
}
