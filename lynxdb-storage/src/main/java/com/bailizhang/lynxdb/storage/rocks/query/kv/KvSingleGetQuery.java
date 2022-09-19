package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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
