package com.bailizhang.lynxdb.storage.rocks.query.kv;

import com.bailizhang.lynxdb.storage.core.ResultSet;
import com.bailizhang.lynxdb.storage.rocks.query.Query;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.List;

public class KvBatchDeleteQuery extends Query<List<byte[]>, Void> {

    public KvBatchDeleteQuery(List<byte[]> queryData, ResultSet<Void> resultSet) {
        super(queryData, resultSet);
    }


    @Override
    public void doQuery(RocksDB db) throws RocksDBException {
        try (final WriteBatch writeBatch = new WriteBatch();
             final WriteOptions writeOptions = new WriteOptions()) {

            for(byte[] key : queryData) {
                writeBatch.delete(key);
            }

            db.write(writeOptions, writeBatch);
        }
    }
}
