package zbl.moonlight.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.*;
import zbl.moonlight.storage.rocks.query.Query;

import java.util.*;

public class KvBatchGetQuery extends Query<List<byte[]>, List<byte[]>> {
    public KvBatchGetQuery(List<byte[]> queryData) {
        super(queryData);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {

    }
}
