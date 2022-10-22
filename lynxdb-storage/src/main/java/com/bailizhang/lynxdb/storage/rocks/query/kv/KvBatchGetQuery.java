package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.storage.core.Pair;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
