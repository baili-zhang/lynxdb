package zbl.moonlight.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.Pair;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.ArrayList;
import java.util.List;

public class KvBatchGetQuery extends Query<List<byte[]>, List<Pair<byte[], byte[]>>> {
    public KvBatchGetQuery(List<byte[]> queryData, ResultSet<List<Pair<byte[], byte[]>>> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        List<Pair<byte[], byte[]>> result = new ArrayList<>();

        for(byte[] key : queryData) {
            byte[] value = db.get(key);
            result.add(new Pair<>(key, value));
        }

        resultSet.setResult(result);
    }
}
