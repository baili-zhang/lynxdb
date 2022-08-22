package zbl.moonlight.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.query.Query;

public class KvSingleGetQuery extends Query<byte[], byte[]> {
    public KvSingleGetQuery(byte[] queryData, ResultSet<byte[]> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        byte[] value = db.get(queryData);
        resultSet.setResult(value);
    }
}
