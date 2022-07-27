package zbl.moonlight.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.List;

public class KvBatchDeleteQuery extends Query<List<byte[]>, Void> {

    protected KvBatchDeleteQuery(List<byte[]> queryData) {
        super(queryData);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {

    }
}
