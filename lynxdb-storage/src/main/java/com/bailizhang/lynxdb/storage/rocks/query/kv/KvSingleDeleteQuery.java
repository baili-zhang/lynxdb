package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class KvSingleDeleteQuery extends Query<byte[], Void> {
    public KvSingleDeleteQuery(byte[] queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }

    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        db.delete(queryData);
    }
}
