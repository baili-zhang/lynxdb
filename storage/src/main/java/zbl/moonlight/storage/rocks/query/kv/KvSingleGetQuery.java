package zbl.moonlight.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.rocks.query.Query;

public class KvSingleGetQuery extends Query<byte[], byte[]> {
    protected KvSingleGetQuery(byte[] queryData) {
        super(queryData);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {

    }
}
