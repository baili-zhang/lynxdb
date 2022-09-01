package com.bailizhang.lynxdb.storage.rocks.query.kv;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;

public class KvSingleDeleteQuery extends Query<byte[], Void> {
    public KvSingleDeleteQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        db.delete(queryData);
    }
}
